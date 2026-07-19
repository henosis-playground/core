//! Local Supabase schema controller.
//!
//! Every schema is reconciled independently from a target receipt keyed by the
//! graph and resource `TypeID`s. Migration receipts use the same ownership key.
//! No process-memory ownership registry participates in observation or cleanup.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use futures::FutureExt as _;
use futures::future::BoxFuture;
use henosis_controller_runtime::PerResourceReconciler;
use henosis_controller_runtime::ReconcileDecision;
use henosis_controller_runtime::ResourceConvergence;
use henosis_controller_runtime::ResourceGoal;
use henosis_controller_runtime::SlicePass;
use henosis_controller_runtime::controller_name;
use henosis_controller_runtime::failed_report;
use henosis_controller_runtime::output;
use henosis_controller_runtime::publication_id;
use henosis_controller_runtime::ready_report;
use henosis_controller_runtime::reconcile_absent;
use henosis_controller_runtime::reconcile_slice;
use henosis_types::BundleRef;
use henosis_types::ComponentName;
use henosis_types::ConfigClosureReader;
use henosis_types::Controller;
use henosis_types::ControllerCommand;
use henosis_types::ControllerError;
use henosis_types::ControllerName;
use henosis_types::ControllerPass;
use henosis_types::ControllerSlice;
use henosis_types::GraphId;
use henosis_types::Resource;
use henosis_types::ResourceId;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest as _;
use sha2::Sha256;
use thiserror::Error;

const CONTROLLER_NAME: &str = "supabase";
const POSTGREST_CONFIG_LOCK: i64 = 0x4845_4e4f_5349_5301;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaBody {
    pub stack: String,
    pub project: String,
    pub database: String,
    pub schema: String,
    pub migrations: Vec<MigrationRef>,
    pub api: ApiPolicy,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MigrationRef {
    pub id: String,
    pub path: String,
    pub sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiPolicy {
    pub expose: bool,
    pub anon_access: AnonymousAccess,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AnonymousAccess {
    None,
    Read,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SupabaseObservation {
    pub schema_exists: bool,
    pub owned_schema: Option<String>,
    pub migrations: BTreeMap<String, String>,
    pub exposed: BTreeSet<String>,
    pub anon_read: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SupabaseOperation {
    EnsureSchema {
        graph: GraphId,
        resource: ResourceId,
        schema: String,
    },
    ApplyMigration {
        graph: GraphId,
        resource: ResourceId,
        schema: String,
        id: String,
        checksum: String,
        sql: String,
    },
    ConfigureApi {
        graph: GraphId,
        resource: ResourceId,
        schema: String,
        observed_digest: String,
        exposed: BTreeSet<String>,
        anon_read: BTreeSet<String>,
    },
    DropSchema {
        graph: GraphId,
        resource: ResourceId,
        schema: String,
    },
}

pub trait ComponentBundleResolver {
    fn resolve(&self, component: &ComponentName) -> Result<BundleRef, SupabaseError>;
}

impl ComponentBundleResolver for ControllerSlice {
    fn resolve(&self, component: &ComponentName) -> Result<BundleRef, SupabaseError> {
        self.bundle(component).ok_or_else(|| {
            SupabaseError::Plan(format!(
                "error[supabase.bundle.missing]: controller slice has no bundle for component \
                 {component}"
            ))
        })
    }
}

pub trait SupabaseTarget: Send + Sync {
    fn observe(
        &self,
        graph: GraphId,
        resource: ResourceId,
        schema: &str,
    ) -> Result<SupabaseObservation, SupabaseError>;
    fn apply(
        &self,
        observed_digest: &str,
        operation: &SupabaseOperation,
    ) -> Result<String, SupabaseError>;
    fn api_url(&self) -> &str;
    fn database_url_ref(&self) -> &str;
    fn anon_key_ref(&self) -> &str;
}

pub struct SupabaseController<T> {
    name: ControllerName,
    target: T,
    config_files: Arc<dyn ConfigClosureReader>,
}

struct SupabasePass<'a, T> {
    controller: &'a SupabaseController<T>,
    slice: Option<&'a ControllerSlice>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupabaseResourceObservation {
    target: SupabaseObservation,
    body: SchemaBody,
    migrations: Vec<PreparedMigration>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedMigration {
    id: String,
    checksum: String,
    sql: String,
}

impl<T> SupabaseController<T>
where
    T: SupabaseTarget,
{
    #[must_use]
    pub fn new(target: T, config_files: Arc<dyn ConfigClosureReader>) -> Self {
        Self {
            name: controller_name(CONTROLLER_NAME),
            target,
            config_files,
        }
    }

    fn for_slice<'a>(&'a self, slice: &'a ControllerSlice) -> SupabasePass<'a, T> {
        SupabasePass {
            controller: self,
            slice: Some(slice),
        }
    }

    async fn reconcile(&self, slice: &ControllerSlice) -> Result<ControllerPass, ControllerError> {
        let pass = self.for_slice(slice);
        match reconcile_slice(&pass, slice).await {
            Ok(SlicePass::Acted) => Ok(ControllerPass::Acted),
            Ok(SlicePass::Converged(convergence)) => ready_report(
                slice,
                Some(publication_id(&convergence.evidence)),
                convergence.outputs,
            )
            .map(|report| ControllerPass::Converged(Some(report)))
            .map_err(|error| ControllerError::new(error.to_string())),
            Err(SupabaseError::Unavailable(message)) => Ok(ControllerPass::Retryable(message)),
            Err(error) => failed_report(slice, error.to_string())
                .map(ControllerPass::Failed)
                .map_err(|report_error| ControllerError::new(report_error.to_string())),
        }
    }
}

impl<T> PerResourceReconciler for SupabasePass<'_, T>
where
    T: SupabaseTarget,
{
    type Action = SupabaseOperation;
    type Error = SupabaseError;
    type Observation = SupabaseResourceObservation;

    fn observe<'a>(
        &'a self,
        graph_id: GraphId,
        resource: &'a Resource,
    ) -> BoxFuture<'a, Result<Self::Observation, Self::Error>> {
        async move {
            let body = decode_body(resource)?;
            let target = self
                .controller
                .target
                .observe(graph_id, resource.id(), &body.schema)?;
            let mut migrations = Vec::with_capacity(body.migrations.len());
            if let Some(slice) = self.slice {
                let bundle = slice.resolve(resource.path().instance())?;
                for migration in &body.migrations {
                    let bytes = self
                        .controller
                        .config_files
                        .read(bundle, &migration.path)
                        .await
                        .map_err(|error| SupabaseError::Plan(error.to_string()))?;
                    let actual = format!("sha256:{}", hex::encode(Sha256::digest(bytes.as_ref())));
                    if actual != migration.sha256 {
                        return Err(SupabaseError::Plan(format!(
                            "error[supabase.plan.checksum]: migration {:?} for {} declares {}, \
                             but file {} hashes to {}",
                            migration.id,
                            resource.path(),
                            migration.sha256,
                            migration.path,
                            actual
                        )));
                    }
                    let sql = String::from_utf8(bytes.to_vec()).map_err(|_| {
                        SupabaseError::Plan(format!(
                            "error[supabase.plan.migration-encoding]: migration {:?} for {} is \
                             not UTF-8",
                            migration.id,
                            resource.path()
                        ))
                    })?;
                    migrations.push(PreparedMigration {
                        id: migration.id.clone(),
                        checksum: migration.sha256.clone(),
                        sql,
                    });
                }
            }
            Ok(SupabaseResourceObservation {
                target,
                body,
                migrations,
            })
        }
        .boxed()
    }

    fn diff(
        &self,
        graph_id: GraphId,
        resource: &Resource,
        goal: ResourceGoal,
        observed: &Self::Observation,
    ) -> Result<ReconcileDecision<Self::Action>, Self::Error> {
        let expected_owner = observed.target.owned_schema.as_deref() == Some(&observed.body.schema);
        if observed.target.schema_exists && !expected_owner {
            return Err(SupabaseError::Provider(format!(
                "refusing to mutate schema {:?} for {} because no matching graph/resource \
                 ownership receipt exists",
                observed.body.schema,
                resource.path()
            )));
        }
        let (desired_exposed, desired_anon) = resource_api(&observed.target, &observed.body, goal);
        match goal {
            ResourceGoal::Absent => {
                if observed.target.exposed != desired_exposed
                    || observed.target.anon_read != desired_anon
                {
                    return Ok(ReconcileDecision::Act(SupabaseOperation::ConfigureApi {
                        graph: graph_id,
                        resource: resource.id(),
                        schema: observed.body.schema.clone(),
                        observed_digest: observation_digest(&observed.target),
                        exposed: desired_exposed,
                        anon_read: desired_anon,
                    }));
                }
                if expected_owner {
                    return Ok(ReconcileDecision::Act(SupabaseOperation::DropSchema {
                        graph: graph_id,
                        resource: resource.id(),
                        schema: observed.body.schema.clone(),
                    }));
                }
                Ok(ReconcileDecision::Converged(ResourceConvergence::default()))
            }
            ResourceGoal::Present => {
                if !observed.target.schema_exists {
                    return Ok(ReconcileDecision::Act(SupabaseOperation::EnsureSchema {
                        graph: graph_id,
                        resource: resource.id(),
                        schema: observed.body.schema.clone(),
                    }));
                }
                for migration in &observed.migrations {
                    if let Some(checksum) = observed.target.migrations.get(&migration.id) {
                        if checksum != &migration.checksum {
                            return Err(SupabaseError::Plan(format!(
                                "error[supabase.plan.migration-mutated]: migration {:?} for {} \
                                 was applied with {}, but desired declares {}\n  = help: never \
                                 edit an applied migration ID; append a corrective migration",
                                migration.id,
                                resource.path(),
                                checksum,
                                migration.checksum
                            )));
                        }
                    } else {
                        return Ok(ReconcileDecision::Act(SupabaseOperation::ApplyMigration {
                            graph: graph_id,
                            resource: resource.id(),
                            schema: observed.body.schema.clone(),
                            id: migration.id.clone(),
                            checksum: migration.checksum.clone(),
                            sql: migration.sql.clone(),
                        }));
                    }
                }
                if observed.target.exposed != desired_exposed
                    || observed.target.anon_read != desired_anon
                {
                    return Ok(ReconcileDecision::Act(SupabaseOperation::ConfigureApi {
                        graph: graph_id,
                        resource: resource.id(),
                        schema: observed.body.schema.clone(),
                        observed_digest: observation_digest(&observed.target),
                        exposed: desired_exposed,
                        anon_read: desired_anon,
                    }));
                }
                Ok(ReconcileDecision::Converged(ResourceConvergence {
                    outputs: resource_outputs(&self.controller.target, resource, &observed.body)?,
                    evidence: observation_digest(&observed.target).into_bytes(),
                }))
            }
        }
    }

    fn act<'a>(
        &'a self,
        _graph_id: GraphId,
        _resource: &'a Resource,
        action: Self::Action,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        async move {
            let observed_digest = match &action {
                SupabaseOperation::EnsureSchema {
                    graph,
                    resource,
                    schema,
                }
                | SupabaseOperation::ApplyMigration {
                    graph,
                    resource,
                    schema,
                    ..
                }
                | SupabaseOperation::DropSchema {
                    graph,
                    resource,
                    schema,
                } => {
                    observation_digest(&self.controller.target.observe(*graph, *resource, schema)?)
                }
                SupabaseOperation::ConfigureApi {
                    observed_digest, ..
                } => observed_digest.clone(),
            };
            self.controller.target.apply(&observed_digest, &action)?;
            Ok(())
        }
        .boxed()
    }
}

impl<T> Controller for SupabaseController<T>
where
    T: SupabaseTarget,
{
    fn name(&self) -> &ControllerName {
        &self.name
    }

    fn execute<'a>(
        &'a self,
        command: &'a ControllerCommand,
    ) -> BoxFuture<'a, Result<ControllerPass, ControllerError>> {
        async move {
            match command {
                ControllerCommand::Reconcile(slice) => self.reconcile(slice).await,
                ControllerCommand::Supersede(supersession) => {
                    let pass = SupabasePass {
                        controller: self,
                        slice: None,
                    };
                    reconcile_absent(&pass, supersession.graph_id, &supersession.resources)
                        .await
                        .map(|pass| match pass {
                            SlicePass::Acted => ControllerPass::Acted,
                            SlicePass::Converged(_) => ControllerPass::Converged(None),
                        })
                        .map_err(|error| ControllerError::new(error.to_string()))
                }
                ControllerCommand::Retire(retirement) => {
                    let pass = SupabasePass {
                        controller: self,
                        slice: None,
                    };
                    reconcile_absent(&pass, retirement.graph_id, &retirement.resources)
                        .await
                        .map(|pass| match pass {
                            SlicePass::Acted => ControllerPass::Acted,
                            SlicePass::Converged(_) => ControllerPass::Converged(None),
                        })
                        .map_err(|error| ControllerError::new(error.to_string()))
                }
            }
        }
        .boxed()
    }
}

fn decode_body(resource: &Resource) -> Result<SchemaBody, SupabaseError> {
    if resource.kind().name().as_str() != "supabase/schema" || resource.kind().version().get() != 1
    {
        return Err(SupabaseError::Plan(format!(
            "error[supabase.kind.unsupported]: {} has unsupported kind {}",
            resource.path(),
            resource.kind()
        )));
    }
    let body: SchemaBody =
        serde_json::from_value(resource.body().as_json().clone()).map_err(|error| {
            SupabaseError::Plan(format!(
                "error[supabase.body.invalid]: {}: {error}",
                resource.path()
            ))
        })?;
    validate_body(resource, &body)?;
    Ok(body)
}

fn resource_api(
    observed: &SupabaseObservation,
    body: &SchemaBody,
    goal: ResourceGoal,
) -> (BTreeSet<String>, BTreeSet<String>) {
    let mut exposed = observed.exposed.clone();
    let mut anon_read = observed.anon_read.clone();
    exposed.insert("public".into());
    exposed.remove(&body.schema);
    anon_read.remove(&body.schema);
    if goal == ResourceGoal::Present && body.api.expose {
        exposed.insert(body.schema.clone());
        if body.api.anon_access == AnonymousAccess::Read {
            anon_read.insert(body.schema.clone());
        }
    }
    (exposed, anon_read)
}

fn resource_outputs(
    target: &impl SupabaseTarget,
    resource: &Resource,
    body: &SchemaBody,
) -> Result<Vec<henosis_types::ObservedOutput>, SupabaseError> {
    let base = target.api_url().trim_end_matches('/');
    let mut outputs = Vec::new();
    for (name, value) in [
        ("project", serde_json::json!(body.project)),
        ("database", serde_json::json!(body.database)),
        ("schema", serde_json::json!(body.schema)),
        ("apiUrl", serde_json::json!(base)),
        ("restUrl", serde_json::json!(format!("{base}/rest/v1"))),
        (
            "databaseUrlRef",
            serde_json::json!(target.database_url_ref()),
        ),
        ("anonKeyRef", serde_json::json!(target.anon_key_ref())),
    ] {
        if resource
            .outputs()
            .any(|declaration| declaration.name().as_str() == name)
        {
            outputs.push(
                output(resource, name, value)
                    .map_err(|error| SupabaseError::Plan(error.to_string()))?,
            );
        }
    }
    Ok(outputs)
}

fn validate_body(resource: &Resource, body: &SchemaBody) -> Result<(), SupabaseError> {
    if body.stack != "local" || body.project != "henosis-local" || body.database != "postgres" {
        return Err(SupabaseError::Plan(format!(
            "error[supabase.target.unsupported]: {} must target local/henosis-local/postgres",
            resource.path()
        )));
    }
    if body.schema.is_empty()
        || !body
            .schema
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(SupabaseError::Plan(format!(
            "error[supabase.schema.invalid]: {} schema {:?} must contain lowercase letters, \
             digits, or underscores",
            resource.path(),
            body.schema
        )));
    }
    let mut ids = BTreeSet::new();
    for migration in &body.migrations {
        if !ids.insert(&migration.id) {
            return Err(SupabaseError::Plan(format!(
                "error[supabase.migration.duplicate]: {} repeats migration ID {:?}",
                resource.path(),
                migration.id
            )));
        }
        if migration.path.starts_with('/')
            || migration.path.split(['/', '\\']).any(|part| part == "..")
        {
            return Err(SupabaseError::Plan(format!(
                "error[supabase.migration.path]: {} migration {:?} path must be \
                 repository-relative without parent traversal",
                resource.path(),
                migration.id
            )));
        }
        if !migration.sha256.starts_with("sha256:") || migration.sha256.len() != 71 {
            return Err(SupabaseError::Plan(format!(
                "error[supabase.migration.checksum]: {} migration {:?} needs a lowercase SHA-256 \
                 digest",
                resource.path(),
                migration.id
            )));
        }
    }
    Ok(())
}

fn observation_digest(observed: &SupabaseObservation) -> String {
    let text = format!(
        "exists={};owned={:?};migrations={:?};exposed={:?};anon={:?}",
        observed.schema_exists,
        observed.owned_schema,
        observed.migrations,
        observed.exposed,
        observed.anon_read
    );
    format!("sha256:{}", hex::encode(Sha256::digest(text.as_bytes())))
}

// === LOCAL POSTGRES/POSTGREST TARGET ===

#[derive(Clone, Debug)]
pub struct LocalSupabaseConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: String,
    pub password_file: PathBuf,
    pub api_url: String,
    pub database_url_ref: String,
    pub anon_key_ref: String,
}

#[derive(Clone, Debug)]
pub struct LocalSupabaseTarget {
    config: LocalSupabaseConfig,
}

impl LocalSupabaseTarget {
    #[must_use]
    pub fn new(config: LocalSupabaseConfig) -> Self {
        Self { config }
    }

    fn connect(&self) -> Result<postgres::Client, SupabaseError> {
        let password = fs::read_to_string(&self.config.password_file)
            .map_err(|error| SupabaseError::Unavailable(error.to_string()))?;
        postgres::Config::new()
            .host(&self.config.host)
            .port(self.config.port)
            .user(&self.config.user)
            .password(password.trim())
            .dbname(&self.config.database)
            .connect(postgres::NoTls)
            .map_err(database_error)
    }
}

impl SupabaseTarget for LocalSupabaseTarget {
    fn observe(
        &self,
        graph: GraphId,
        resource: ResourceId,
        schema: &str,
    ) -> Result<SupabaseObservation, SupabaseError> {
        observe_database(&mut self.connect()?, graph, resource, schema)
    }

    fn apply(
        &self,
        observed_digest: &str,
        operation: &SupabaseOperation,
    ) -> Result<String, SupabaseError> {
        let mut client = self.connect()?;
        let mut transaction = client.transaction().map_err(database_error)?;
        if matches!(operation, SupabaseOperation::ConfigureApi { .. }) {
            transaction
                .query_one(
                    "select pg_advisory_xact_lock($1)",
                    &[&POSTGREST_CONFIG_LOCK],
                )
                .map_err(database_error)?;
        }
        if let Some((graph, resource, schema)) = operation_identity(operation) {
            let fresh = observe_database(&mut transaction, graph, resource, schema)?;
            if observation_digest(&fresh) != observed_digest {
                return Err(SupabaseError::Unavailable(
                    "target changed after observation; retry from fresh state".into(),
                ));
            }
        }
        ensure_metadata(&mut transaction)?;
        match operation {
            SupabaseOperation::EnsureSchema {
                graph,
                resource,
                schema,
            } => {
                transaction
                    .batch_execute(&format!(
                        "create schema {}; revoke all on schema {} from public;",
                        quote_identifier(schema),
                        quote_identifier(schema)
                    ))
                    .map_err(database_error)?;
                transaction
                    .execute(
                        "insert into henosis_controller.owned_schemas (graph_id, resource_id, \
                         schema_name) values ($1, $2, $3) on conflict (graph_id, resource_id) do \
                         update set schema_name = excluded.schema_name",
                        &[&graph.to_string(), &resource.to_string(), schema],
                    )
                    .map_err(database_error)?;
            }
            SupabaseOperation::ApplyMigration {
                graph,
                resource,
                schema,
                id,
                checksum,
                sql,
            } => {
                transaction
                    .batch_execute(&format!(
                        "set local search_path = {}, public, extensions;",
                        quote_identifier(schema)
                    ))
                    .map_err(database_error)?;
                transaction.batch_execute(sql).map_err(database_error)?;
                transaction
                    .execute(
                        "insert into henosis_controller.migration_receipts (graph_id, \
                         resource_id, migration_id, checksum) values ($1, $2, $3, $4)",
                        &[&graph.to_string(), &resource.to_string(), id, checksum],
                    )
                    .map_err(database_error)?;
            }
            SupabaseOperation::ConfigureApi {
                exposed, anon_read, ..
            } => {
                configure_api(&mut transaction, exposed, anon_read)?;
            }
            SupabaseOperation::DropSchema {
                graph,
                resource,
                schema,
            } => {
                transaction
                    .batch_execute(&format!(
                        "drop schema {} cascade;",
                        quote_identifier(schema)
                    ))
                    .map_err(database_error)?;
                transaction
                    .execute(
                        "delete from henosis_controller.migration_receipts where graph_id = $1 \
                         and resource_id = $2",
                        &[&graph.to_string(), &resource.to_string()],
                    )
                    .map_err(database_error)?;
                transaction
                    .execute(
                        "delete from henosis_controller.owned_schemas where graph_id = $1 and \
                         resource_id = $2",
                        &[&graph.to_string(), &resource.to_string()],
                    )
                    .map_err(database_error)?;
            }
        }
        transaction.commit().map_err(database_error)?;
        Ok("applied".into())
    }

    fn api_url(&self) -> &str {
        &self.config.api_url
    }

    fn database_url_ref(&self) -> &str {
        &self.config.database_url_ref
    }

    fn anon_key_ref(&self) -> &str {
        &self.config.anon_key_ref
    }
}

fn operation_identity(operation: &SupabaseOperation) -> Option<(GraphId, ResourceId, &str)> {
    match operation {
        SupabaseOperation::EnsureSchema {
            graph,
            resource,
            schema,
        }
        | SupabaseOperation::ApplyMigration {
            graph,
            resource,
            schema,
            ..
        }
        | SupabaseOperation::DropSchema {
            graph,
            resource,
            schema,
        }
        | SupabaseOperation::ConfigureApi {
            graph,
            resource,
            schema,
            ..
        } => Some((*graph, *resource, schema)),
    }
}

fn ensure_metadata(client: &mut impl postgres::GenericClient) -> Result<(), SupabaseError> {
    client
        .batch_execute(
            "create schema if not exists henosis_controller; create table if not exists \
             henosis_controller.owned_schemas (graph_id text not null, resource_id text not null, \
             schema_name text unique not null, primary key (graph_id, resource_id)); create table \
             if not exists henosis_controller.migration_receipts (graph_id text not null, \
             resource_id text not null, migration_id text not null, checksum text not null, \
             applied_at timestamptz not null default now(), primary key (graph_id, resource_id, \
             migration_id)); revoke all on schema henosis_controller from public, anon, \
             authenticated;",
        )
        .map_err(database_error)
}

fn observe_database(
    client: &mut impl postgres::GenericClient,
    graph: GraphId,
    resource: ResourceId,
    schema: &str,
) -> Result<SupabaseObservation, SupabaseError> {
    let schema_exists: bool = client
        .query_one("select to_regnamespace($1) is not null", &[&schema])
        .map_err(database_error)?
        .get(0);
    let metadata_exists: bool = client
        .query_one(
            "select to_regclass('henosis_controller.owned_schemas') is not null",
            &[],
        )
        .map_err(database_error)?
        .get(0);
    let mut observation = SupabaseObservation {
        schema_exists,
        ..SupabaseObservation::default()
    };
    if metadata_exists {
        observation.owned_schema = client
            .query_opt(
                "select schema_name from henosis_controller.owned_schemas where graph_id = $1 and \
                 resource_id = $2",
                &[&graph.to_string(), &resource.to_string()],
            )
            .map_err(database_error)?
            .map(|row| row.get(0));
        for row in client
            .query(
                "select migration_id, checksum from henosis_controller.migration_receipts where \
                 graph_id = $1 and resource_id = $2 order by migration_id",
                &[&graph.to_string(), &resource.to_string()],
            )
            .map_err(database_error)?
        {
            observation.migrations.insert(row.get(0), row.get(1));
        }
    }
    let configured: String = client
        .query_one(
            "select coalesce((select split_part(setting, '=', 2) from pg_roles cross join lateral \
             unnest(coalesce(rolconfig, '{}'::text[])) setting where rolname = 'postgres' and \
             setting like 'pgrst.db_schemas=%' limit 1), 'public')",
            &[],
        )
        .map_err(database_error)?
        .get(0);
    observation.exposed = configured
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect();
    for exposed_schema in &observation.exposed {
        let read: bool = client
            .query_one(
                "select has_schema_privilege('anon', $1, 'USAGE')",
                &[exposed_schema],
            )
            .map_err(database_error)?
            .get(0);
        if read {
            observation.anon_read.insert(exposed_schema.clone());
        }
    }
    Ok(observation)
}

fn configure_api(
    client: &mut impl postgres::GenericClient,
    exposed: &BTreeSet<String>,
    anon_read: &BTreeSet<String>,
) -> Result<(), SupabaseError> {
    let schemas = exposed.iter().cloned().collect::<Vec<_>>().join(",");
    client
        .batch_execute(&format!(
            "alter role postgres set pgrst.db_schemas = {};",
            quote_literal(&schemas)
        ))
        .map_err(database_error)?;
    for schema in exposed.iter().filter(|schema| schema.as_str() != "public") {
        let quoted = quote_identifier(schema);
        if anon_read.contains(schema) {
            client
                .batch_execute(&format!(
                    "grant usage on schema {quoted} to anon; grant select on all tables in schema \
                     {quoted} to anon; alter default privileges in schema {quoted} grant select \
                     on tables to anon;"
                ))
                .map_err(database_error)?;
        } else {
            client
                .batch_execute(&format!(
                    "revoke select on all tables in schema {quoted} from anon; revoke usage on \
                     schema {quoted} from anon; alter default privileges in schema {quoted} \
                     revoke select on tables from anon;"
                ))
                .map_err(database_error)?;
        }
    }
    client
        .batch_execute("notify pgrst, 'reload config'; notify pgrst, 'reload schema';")
        .map_err(database_error)
}

fn database_error(error: postgres::Error) -> SupabaseError {
    let Some(database) = error.as_db_error() else {
        return SupabaseError::Unavailable(error.to_string());
    };
    let mut detail = format!(
        "code: {}\nmessage: {}",
        database.code().code(),
        database.message()
    );
    if let Some(provider_detail) = database.detail() {
        detail.push_str(&format!("\ndetail: {provider_detail}"));
    }
    if let Some(hint) = database.hint() {
        detail.push_str(&format!("\nhint: {hint}"));
    }
    SupabaseError::Provider(detail)
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SupabaseError {
    #[error("{0}")]
    Plan(String),
    #[error("local Supabase unavailable: {0}")]
    Unavailable(String),
    #[error("local Supabase rejected the operation: {0}")]
    Provider(String),
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::sync::Mutex;

    use henosis_types::ContentDigest;
    use henosis_types::Generation;
    use henosis_types::KindName;
    use henosis_types::KindVersion;
    use henosis_types::NewResource;
    use henosis_types::OutputAvailability;
    use henosis_types::OutputDeclaration;
    use henosis_types::OutputName;
    use henosis_types::ResourceAddress;
    use henosis_types::ResourceName;
    use henosis_types::ResourcePath;
    use henosis_types::Retirement;

    use super::*;

    #[derive(Default)]
    struct FakeState {
        schemas: BTreeMap<(GraphId, ResourceId), String>,
        migrations: BTreeMap<(GraphId, ResourceId, String), String>,
        exposed: BTreeSet<String>,
        anon_read: BTreeSet<String>,
    }

    struct FakeTarget {
        state: Arc<Mutex<FakeState>>,
        actions: Arc<Mutex<Vec<SupabaseOperation>>>,
    }

    struct FakeConfigFiles {
        sql: Arc<[u8]>,
    }

    impl ConfigClosureReader for FakeConfigFiles {
        fn read<'a>(
            &'a self,
            _bundle: BundleRef,
            path: &'a str,
        ) -> BoxFuture<'a, Result<Arc<[u8]>, henosis_types::ConfigClosureError>> {
            async move {
                if path == "migrations/001.sql" {
                    Ok(Arc::clone(&self.sql))
                } else {
                    Err(henosis_types::ConfigClosureError::Missing {
                        bundle: BundleRef::new(ContentDigest::digest(b"bundle")),
                        path: path.to_owned(),
                    })
                }
            }
            .boxed()
        }
    }

    impl SupabaseTarget for FakeTarget {
        fn observe(
            &self,
            graph: GraphId,
            resource: ResourceId,
            schema: &str,
        ) -> Result<SupabaseObservation, SupabaseError> {
            let state = self.state.lock().unwrap();
            let owned_schema = state.schemas.get(&(graph, resource)).cloned();
            Ok(SupabaseObservation {
                schema_exists: state.schemas.values().any(|value| value == schema),
                owned_schema,
                migrations: state
                    .migrations
                    .iter()
                    .filter(|((g, r, _), _)| *g == graph && *r == resource)
                    .map(|((_, _, id), checksum)| (id.clone(), checksum.clone()))
                    .collect(),
                exposed: state.exposed.clone(),
                anon_read: state.anon_read.clone(),
            })
        }

        fn apply(
            &self,
            observed_digest: &str,
            operation: &SupabaseOperation,
        ) -> Result<String, SupabaseError> {
            let mut state = self.state.lock().unwrap();
            if let Some((graph, resource, schema)) = operation_identity(operation) {
                let fresh = SupabaseObservation {
                    schema_exists: state.schemas.values().any(|value| value == schema),
                    owned_schema: state.schemas.get(&(graph, resource)).cloned(),
                    migrations: state
                        .migrations
                        .iter()
                        .filter(|((g, r, _), _)| *g == graph && *r == resource)
                        .map(|((_, _, id), checksum)| (id.clone(), checksum.clone()))
                        .collect(),
                    exposed: state.exposed.clone(),
                    anon_read: state.anon_read.clone(),
                };
                if observation_digest(&fresh) != observed_digest {
                    return Err(SupabaseError::Unavailable(
                        "target changed after observation; retry from fresh state".into(),
                    ));
                }
            }
            self.actions.lock().unwrap().push(operation.clone());
            match operation {
                SupabaseOperation::EnsureSchema {
                    graph,
                    resource,
                    schema,
                } => {
                    state.schemas.insert((*graph, *resource), schema.clone());
                }
                SupabaseOperation::ApplyMigration {
                    graph,
                    resource,
                    id,
                    checksum,
                    ..
                } => {
                    state
                        .migrations
                        .insert((*graph, *resource, id.clone()), checksum.clone());
                }
                SupabaseOperation::ConfigureApi {
                    exposed, anon_read, ..
                } => {
                    state.exposed = exposed.clone();
                    state.anon_read = anon_read.clone();
                }
                SupabaseOperation::DropSchema {
                    graph, resource, ..
                } => {
                    state.schemas.remove(&(*graph, *resource));
                    state
                        .migrations
                        .retain(|(g, r, _), _| g != graph || r != resource);
                }
            }
            Ok("applied".into())
        }

        fn api_url(&self) -> &str {
            "http://localhost:4484"
        }

        fn database_url_ref(&self) -> &str {
            "docker-secret://supabase-connection-url"
        }

        fn anon_key_ref(&self) -> &str {
            "docker-secret://supabase-anon-key"
        }
    }

    #[tokio::test]
    async fn converges_one_operation_per_pass_without_flapping() {
        let (controller, actions) = controller();
        let slice = slice();
        for expected in ["ensure schema", "apply migration", "configure API"] {
            assert_eq!(
                reconcile_slice(&controller.for_slice(&slice), &slice)
                    .await
                    .unwrap(),
                SlicePass::Acted,
                "{expected} is the sole action in its pass"
            );
        }
        assert!(matches!(
            actions.lock().unwrap()[0],
            SupabaseOperation::EnsureSchema { .. }
        ));
        assert!(matches!(
            actions.lock().unwrap()[1],
            SupabaseOperation::ApplyMigration { .. }
        ));
        assert!(matches!(
            actions.lock().unwrap()[2],
            SupabaseOperation::ConfigureApi { .. }
        ));
        assert!(matches!(
            reconcile_slice(&controller.for_slice(&slice), &slice)
                .await
                .unwrap(),
            SlicePass::Converged(_)
        ));
        assert_eq!(actions.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn stale_postgrest_configuration_is_fenced() {
        let (controller, actions) = controller();
        let slice = slice();
        assert_eq!(
            reconcile_slice(&controller.for_slice(&slice), &slice)
                .await
                .unwrap(),
            SlicePass::Acted
        );
        assert_eq!(
            reconcile_slice(&controller.for_slice(&slice), &slice)
                .await
                .unwrap(),
            SlicePass::Acted
        );
        let resource = &slice.resources()[0];
        let pass = controller.for_slice(&slice);
        let observed = pass.observe(slice.graph_id(), resource).await.unwrap();
        let ReconcileDecision::Act(action) = pass
            .diff(slice.graph_id(), resource, ResourceGoal::Present, &observed)
            .unwrap()
        else {
            panic!("API exposure still needs one action");
        };
        controller
            .target
            .state
            .lock()
            .unwrap()
            .exposed
            .insert("other_graph".into());
        let error = pass
            .act(slice.graph_id(), resource, action)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("target changed after observation")
        );
        assert_eq!(actions.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn fresh_controller_retires_from_receipt() {
        let state = Arc::new(Mutex::new(FakeState {
            exposed: BTreeSet::from(["public".into()]),
            ..FakeState::default()
        }));
        let actions = Arc::new(Mutex::new(Vec::new()));
        let slice = slice();
        let first = make_controller(Arc::clone(&state), Arc::clone(&actions));
        let reconcile = ControllerCommand::Reconcile(slice.clone());
        for _ in 0..3 {
            assert_eq!(
                first.execute(&reconcile).await.unwrap(),
                ControllerPass::Acted
            );
        }
        assert!(matches!(
            first.execute(&reconcile).await.unwrap(),
            ControllerPass::Converged(Some(_))
        ));
        let restarted = make_controller(Arc::clone(&state), actions);
        let retire = ControllerCommand::Retire(Retirement {
            graph_id: slice.graph_id(),
            last_generation: slice.generation(),
            controller: restarted.name().clone(),
            resources: slice.resources().to_vec(),
        });
        assert_eq!(
            restarted.execute(&retire).await.unwrap(),
            ControllerPass::Acted
        );
        assert_eq!(
            restarted.execute(&retire).await.unwrap(),
            ControllerPass::Acted
        );
        assert_eq!(
            restarted.execute(&retire).await.unwrap(),
            ControllerPass::Converged(None)
        );
        assert!(state.lock().unwrap().schemas.is_empty());
    }

    #[tokio::test]
    async fn refuses_schema_without_matching_receipt() {
        let (controller, actions) = controller();
        let slice = slice();
        controller.target.state.lock().unwrap().schemas.insert(
            (
                GraphId::from_bytes([9; 16]),
                ResourceId::from_bytes([9; 16]),
            ),
            "catalog".into(),
        );
        let error = reconcile_slice(&controller.for_slice(&slice), &slice)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("no matching graph/resource ownership receipt")
        );
        assert!(actions.lock().unwrap().is_empty());
    }

    fn controller() -> (
        SupabaseController<FakeTarget>,
        Arc<Mutex<Vec<SupabaseOperation>>>,
    ) {
        let state = Arc::new(Mutex::new(FakeState {
            exposed: BTreeSet::from(["public".into()]),
            ..FakeState::default()
        }));
        let actions = Arc::new(Mutex::new(Vec::new()));
        (make_controller(state, Arc::clone(&actions)), actions)
    }

    fn make_controller(
        state: Arc<Mutex<FakeState>>,
        actions: Arc<Mutex<Vec<SupabaseOperation>>>,
    ) -> SupabaseController<FakeTarget> {
        let sql = "create table items (id bigint primary key);";
        SupabaseController::new(
            FakeTarget { state, actions },
            Arc::new(FakeConfigFiles {
                sql: Arc::from(sql.as_bytes()),
            }),
        )
    }

    fn slice() -> ControllerSlice {
        let sql = "create table items (id bigint primary key);";
        let checksum = format!("sha256:{}", hex::encode(Sha256::digest(sql.as_bytes())));
        let outputs = [
            "project",
            "database",
            "schema",
            "apiUrl",
            "restUrl",
            "databaseUrlRef",
            "anonKeyRef",
        ]
        .into_iter()
        .map(|name| {
            OutputDeclaration::new(OutputName::new(name).unwrap(), OutputAvailability::Observed)
        })
        .collect();
        let resource = Resource::new(NewResource {
            id: ResourceId::from_bytes([4; 16]),
            path: ResourcePath::new(
                ComponentName::new("catalog").unwrap(),
                ResourceAddress::new(
                    KindVersion::new(
                        KindName::new("supabase/schema").unwrap(),
                        NonZeroU32::new(1).unwrap(),
                    ),
                    ResourceName::new("catalog").unwrap(),
                ),
            ),
            controller: controller_name(CONTROLLER_NAME),
            body: serde_json::json!({"stack":"local","project":"henosis-local","database":"postgres","schema":"catalog","migrations":[{"id":"001","path":"migrations/001.sql","sha256":checksum}],"api":{"expose":true,"anonAccess":"read"}})
                .try_into()
                .unwrap(),
            outputs,
        })
        .unwrap();
        ControllerSlice::new(
            GraphId::from_bytes([3; 16]),
            Generation::new(1).unwrap(),
            ContentDigest::digest(b"plan"),
            controller_name(CONTROLLER_NAME),
            BTreeMap::from([(
                ComponentName::new("catalog").unwrap(),
                BundleRef::new(ContentDigest::digest(b"bundle")),
            )]),
            vec![resource],
            Vec::new(),
        )
    }
}
