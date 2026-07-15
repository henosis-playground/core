//! Local Supabase schema controller.
//!
//! Reconciliation keeps the proven plan/apply split: desired resources and
//! fresh target receipts produce an immutable ordered plan, and only that plan
//! is handed to the target. Applied migration IDs are immutable and
//! checksummed. The target boundary resolves repository-relative migration
//! files because `supabase/schema@1` carries path plus digest, not SQL bytes.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;

use futures::FutureExt as _;
use futures::future::BoxFuture;
use henosis_controller_runtime::controller_name;
use henosis_controller_runtime::failed_report;
use henosis_controller_runtime::output;
use henosis_controller_runtime::publication_id;
use henosis_controller_runtime::ready_report;
use henosis_types::Controller;
use henosis_types::ControllerCommand;
use henosis_types::ControllerError;
use henosis_types::ControllerName;
use henosis_types::ControllerReport;
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
    pub schemas: BTreeSet<String>,
    pub migrations: BTreeMap<(ResourceId, String), String>,
    pub exposed: BTreeSet<String>,
    pub anon_read: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupabasePlan {
    pub graph: GraphId,
    pub generation: u64,
    pub observed_digest: String,
    pub operations: Vec<SupabaseOperation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SupabaseOperation {
    EnsureSchema {
        resource: ResourceId,
        schema: String,
    },
    ApplyMigration {
        resource: ResourceId,
        schema: String,
        id: String,
        checksum: String,
        sql: String,
    },
    ConfigureApi {
        exposed: BTreeSet<String>,
        anon_read: BTreeSet<String>,
    },
}

pub trait SupabaseTarget: Send + Sync {
    fn observe(&self, graph: GraphId) -> Result<SupabaseObservation, SupabaseError>;
    fn migration_sql(&self, path: &str) -> Result<String, SupabaseError>;
    fn apply(&self, plan: &SupabasePlan) -> Result<String, SupabaseError>;
    fn retire(&self, graph: GraphId, resources: &[ResourceId]) -> Result<(), SupabaseError>;
    fn api_url(&self) -> &str;
    fn database_url_ref(&self) -> &str;
    fn anon_key_ref(&self) -> &str;
}

pub struct SupabaseController<T> {
    name: ControllerName,
    target: T,
    state: Mutex<BTreeMap<GraphId, Vec<ResourceId>>>,
}

impl<T> SupabaseController<T>
where
    T: SupabaseTarget,
{
    #[must_use]
    pub fn new(target: T) -> Self {
        Self {
            name: controller_name(CONTROLLER_NAME),
            target,
            state: Mutex::new(BTreeMap::new()),
        }
    }

    fn reconcile(&self, slice: &ControllerSlice) -> Result<ControllerReport, ControllerError> {
        let result = self.plan_and_apply(slice);
        let (outputs, evidence) = match result {
            Ok(result) => result,
            Err(error) => {
                return failed_report(slice, error.to_string())
                    .map_err(|report_error| ControllerError::new(report_error.to_string()));
            }
        };
        self.state
            .lock()
            .expect("supabase controller state lock is not poisoned")
            .insert(
                slice.graph_id(),
                slice.resources().iter().map(Resource::id).collect(),
            );
        ready_report(slice, Some(publication_id(evidence.as_bytes())), outputs)
            .map_err(|error| ControllerError::new(error.to_string()))
    }

    fn plan_and_apply(
        &self,
        slice: &ControllerSlice,
    ) -> Result<(Vec<henosis_types::ObservedOutput>, String), SupabaseError> {
        let observed = self.target.observe(slice.graph_id())?;
        let (plan, bodies) = build_plan(slice, &observed, &self.target)?;
        let evidence = if plan.operations.is_empty() {
            plan.observed_digest.clone()
        } else {
            self.target.apply(&plan)?
        };
        let mut outputs = Vec::new();
        for (resource, body) in slice.resources().iter().zip(bodies) {
            let base = self.target.api_url().trim_end_matches('/');
            for (name, value) in [
                ("project", serde_json::json!(body.project)),
                ("database", serde_json::json!(body.database)),
                ("schema", serde_json::json!(body.schema)),
                ("apiUrl", serde_json::json!(base)),
                ("restUrl", serde_json::json!(format!("{base}/rest/v1"))),
                (
                    "databaseUrlRef",
                    serde_json::json!(self.target.database_url_ref()),
                ),
                ("anonKeyRef", serde_json::json!(self.target.anon_key_ref())),
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
        }
        Ok((outputs, evidence))
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
    ) -> BoxFuture<'a, Result<Option<ControllerReport>, ControllerError>> {
        async move {
            match command {
                ControllerCommand::Reconcile(slice) => self.reconcile(slice).map(Some),
                ControllerCommand::Supersede(supersession) => {
                    self.target
                        .retire(supersession.graph_id, &supersession.resources)
                        .map_err(|error| ControllerError::new(error.to_string()))?;
                    Ok(None)
                }
                ControllerCommand::Retire(retirement) => {
                    self.target
                        .retire(retirement.graph_id, &retirement.resources)
                        .map_err(|error| ControllerError::new(error.to_string()))?;
                    self.state
                        .lock()
                        .expect("supabase controller state lock is not poisoned")
                        .remove(&retirement.graph_id);
                    Ok(None)
                }
            }
        }
        .boxed()
    }
}

fn build_plan<T>(
    slice: &ControllerSlice,
    observed: &SupabaseObservation,
    target: &T,
) -> Result<(SupabasePlan, Vec<SchemaBody>), SupabaseError>
where
    T: SupabaseTarget,
{
    let mut operations = Vec::new();
    let mut bodies = Vec::new();
    let mut exposed = BTreeSet::from(["public".into()]);
    let mut anon_read = BTreeSet::new();
    for resource in slice.resources() {
        if resource.kind().name().as_str() != "supabase/schema"
            || resource.kind().version().get() != 1
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
        if !observed.schemas.contains(&body.schema) {
            operations.push(SupabaseOperation::EnsureSchema {
                resource: resource.id(),
                schema: body.schema.clone(),
            });
        }
        for migration in &body.migrations {
            let key = (resource.id(), migration.id.clone());
            if let Some(checksum) = observed.migrations.get(&key) {
                if checksum != &migration.sha256 {
                    return Err(SupabaseError::Plan(format!(
                        "error[supabase.plan.migration-mutated]: migration {:?} for {} was \
                         applied with {}, but desired declares {}\n  --> {}\n  = help: never edit \
                         an applied migration ID; append a corrective migration",
                        migration.id,
                        resource.path(),
                        checksum,
                        migration.sha256,
                        migration.path
                    )));
                }
                continue;
            }
            let sql = target.migration_sql(&migration.path)?;
            let actual = format!("sha256:{}", hex::encode(Sha256::digest(sql.as_bytes())));
            if actual != migration.sha256 {
                return Err(SupabaseError::Plan(format!(
                    "error[supabase.plan.checksum]: migration {:?} for {} declares {}, but file \
                     {} hashes to {}\n  = help: regenerate the migration reference from the exact \
                     committed SQL bytes",
                    migration.id,
                    resource.path(),
                    migration.sha256,
                    migration.path,
                    actual
                )));
            }
            operations.push(SupabaseOperation::ApplyMigration {
                resource: resource.id(),
                schema: body.schema.clone(),
                id: migration.id.clone(),
                checksum: migration.sha256.clone(),
                sql,
            });
        }
        if body.api.expose {
            exposed.insert(body.schema.clone());
            if body.api.anon_access == AnonymousAccess::Read {
                anon_read.insert(body.schema.clone());
            }
        }
        bodies.push(body);
    }
    if observed.exposed != exposed || observed.anon_read != anon_read {
        operations.push(SupabaseOperation::ConfigureApi { exposed, anon_read });
    }
    let observed_digest = observation_digest(observed);
    Ok((
        SupabasePlan {
            graph: slice.graph_id(),
            generation: slice.generation().ordinal(),
            observed_digest,
            operations,
        },
        bodies,
    ))
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
        "schemas={:?};migrations={:?};exposed={:?};anon={:?}",
        observed.schemas, observed.migrations, observed.exposed, observed.anon_read
    );
    format!("sha256:{}", hex::encode(Sha256::digest(text.as_bytes())))
}

// === LOCAL POSTGRES/POSTGREST TARGET ===

#[derive(Clone, Debug)]
pub struct LocalSupabaseConfig {
    pub connection_url_file: PathBuf,
    pub migration_root: PathBuf,
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
        let connection_url = fs::read_to_string(&self.config.connection_url_file)
            .map_err(|error| SupabaseError::Unavailable(error.to_string()))?;
        postgres::Client::connect(connection_url.trim(), postgres::NoTls).map_err(database_error)
    }
}

impl SupabaseTarget for LocalSupabaseTarget {
    fn observe(&self, _graph: GraphId) -> Result<SupabaseObservation, SupabaseError> {
        observe_database(&mut self.connect()?)
    }

    fn migration_sql(&self, path: &str) -> Result<String, SupabaseError> {
        fs::read_to_string(self.config.migration_root.join(path)).map_err(|error| {
            SupabaseError::Plan(format!("cannot read migration {path:?}: {error}"))
        })
    }

    fn apply(&self, plan: &SupabasePlan) -> Result<String, SupabaseError> {
        let mut client = self.connect()?;
        let mut transaction = client.transaction().map_err(database_error)?;
        let fresh = observe_database(&mut transaction)?;
        if observation_digest(&fresh) != plan.observed_digest {
            return Err(SupabaseError::Unavailable(
                "target changed after planning; retry from fresh observation".into(),
            ));
        }
        ensure_metadata(&mut transaction)?;
        for operation in &plan.operations {
            match operation {
                SupabaseOperation::EnsureSchema { resource, schema } => {
                    transaction
                        .batch_execute(&format!(
                            "create schema if not exists {}; revoke all on schema {} from public;",
                            quote_identifier(schema),
                            quote_identifier(schema)
                        ))
                        .map_err(database_error)?;
                    transaction
                        .execute(
                            "insert into henosis_controller.owned_schemas (resource_id, \
                             schema_name) values ($1, $2) on conflict (resource_id) do update set \
                             schema_name = excluded.schema_name",
                            &[&resource.to_string(), schema],
                        )
                        .map_err(database_error)?;
                }
                SupabaseOperation::ApplyMigration {
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
                            "insert into henosis_controller.migration_receipts (resource_id, \
                             migration_id, checksum) values ($1, $2, $3)",
                            &[&resource.to_string(), id, checksum],
                        )
                        .map_err(database_error)?;
                }
                SupabaseOperation::ConfigureApi { exposed, anon_read } => {
                    configure_api(&mut transaction, exposed, anon_read)?;
                }
            }
        }
        transaction.commit().map_err(database_error)?;
        let observed = observe_database(&mut self.connect()?)?;
        Ok(observation_digest(&observed))
    }

    fn retire(&self, _graph: GraphId, resources: &[ResourceId]) -> Result<(), SupabaseError> {
        let mut client = self.connect()?;
        let mut transaction = client.transaction().map_err(database_error)?;
        let metadata_exists: bool = transaction
            .query_one(
                "select to_regclass('henosis_controller.owned_schemas') is not null",
                &[],
            )
            .map_err(database_error)?
            .get(0);
        if !metadata_exists {
            return Ok(());
        }
        for resource in resources {
            let resource = resource.to_string();
            let schema = transaction
                .query_opt(
                    "select schema_name from henosis_controller.owned_schemas where resource_id = \
                     $1",
                    &[&resource],
                )
                .map_err(database_error)?
                .map(|row| row.get::<_, String>(0));
            if let Some(schema) = schema {
                transaction
                    .batch_execute(&format!(
                        "drop schema if exists {} cascade;",
                        quote_identifier(&schema)
                    ))
                    .map_err(database_error)?;
            }
            transaction
                .execute(
                    "delete from henosis_controller.migration_receipts where resource_id = $1",
                    &[&resource],
                )
                .map_err(database_error)?;
            transaction
                .execute(
                    "delete from henosis_controller.owned_schemas where resource_id = $1",
                    &[&resource],
                )
                .map_err(database_error)?;
        }
        transaction.commit().map_err(database_error)
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

fn ensure_metadata(client: &mut impl postgres::GenericClient) -> Result<(), SupabaseError> {
    client
        .batch_execute(
            "create schema if not exists henosis_controller; create table if not exists \
             henosis_controller.owned_schemas (resource_id text primary key, schema_name text \
             unique not null); create table if not exists henosis_controller.migration_receipts \
             (resource_id text not null, migration_id text not null, checksum text not null, \
             applied_at timestamptz not null default now(), primary key (resource_id, \
             migration_id)); revoke all on schema henosis_controller from public, anon, \
             authenticated;",
        )
        .map_err(database_error)
}

fn observe_database(
    client: &mut impl postgres::GenericClient,
) -> Result<SupabaseObservation, SupabaseError> {
    let metadata_exists: bool = client
        .query_one(
            "select to_regclass('henosis_controller.owned_schemas') is not null",
            &[],
        )
        .map_err(database_error)?
        .get(0);
    let mut observation = SupabaseObservation::default();
    if metadata_exists {
        for row in client
            .query(
                "select resource_id, schema_name from henosis_controller.owned_schemas order by \
                 resource_id",
                &[],
            )
            .map_err(database_error)?
        {
            let schema = row.get::<_, String>(1);
            observation.schemas.insert(schema.clone());
            let read: bool = client
                .query_one(
                    "select has_schema_privilege('anon', $1, 'USAGE')",
                    &[&schema],
                )
                .map_err(database_error)?
                .get(0);
            if read {
                observation.anon_read.insert(schema);
            }
        }
        for row in client
            .query(
                "select resource_id, migration_id, checksum from \
                 henosis_controller.migration_receipts order by resource_id, migration_id",
                &[],
            )
            .map_err(database_error)?
        {
            let resource = row
                .get::<_, String>(0)
                .parse::<ResourceId>()
                .map_err(|error| SupabaseError::Provider(error.to_string()))?;
            observation
                .migrations
                .insert((resource, row.get(1)), row.get(2));
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
        let schema = quote_identifier(schema);
        if anon_read.contains(schema.trim_matches('"')) {
            client
                .batch_execute(&format!(
                    "grant usage on schema {schema} to anon; grant select on all tables in schema \
                     {schema} to anon; alter default privileges in schema {schema} grant select \
                     on tables to anon;"
                ))
                .map_err(database_error)?;
        } else {
            client
                .batch_execute(&format!(
                    "revoke select on all tables in schema {schema} from anon; revoke usage on \
                     schema {schema} from anon; alter default privileges in schema {schema} \
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
    #[error("local Supabase rejected the plan: {0}")]
    Provider(String),
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use henosis_types::ComponentName;
    use henosis_types::ContentDigest;
    use henosis_types::ControllerSlice;
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

    struct FakeTarget {
        observation: Mutex<SupabaseObservation>,
        sql: String,
        applies: Mutex<usize>,
        retired: Mutex<Vec<ResourceId>>,
    }

    impl SupabaseTarget for FakeTarget {
        fn observe(&self, _graph: GraphId) -> Result<SupabaseObservation, SupabaseError> {
            Ok(self.observation.lock().unwrap().clone())
        }

        fn migration_sql(&self, _path: &str) -> Result<String, SupabaseError> {
            Ok(self.sql.clone())
        }

        fn apply(&self, plan: &SupabasePlan) -> Result<String, SupabaseError> {
            *self.applies.lock().unwrap() += 1;
            let mut observed = self.observation.lock().unwrap();
            for operation in &plan.operations {
                match operation {
                    SupabaseOperation::EnsureSchema { schema, .. } => {
                        observed.schemas.insert(schema.clone());
                    }
                    SupabaseOperation::ApplyMigration {
                        resource,
                        id,
                        checksum,
                        ..
                    } => {
                        observed
                            .migrations
                            .insert((*resource, id.clone()), checksum.clone());
                    }
                    SupabaseOperation::ConfigureApi { exposed, anon_read } => {
                        observed.exposed = exposed.clone();
                        observed.anon_read = anon_read.clone();
                    }
                }
            }
            Ok(observation_digest(&observed))
        }

        fn retire(&self, _graph: GraphId, resources: &[ResourceId]) -> Result<(), SupabaseError> {
            self.retired.lock().unwrap().extend_from_slice(resources);
            Ok(())
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
    async fn plans_applies_reports_idempotently_and_retires() {
        let sql = "create table items (id bigint primary key);".to_owned();
        let controller = SupabaseController::new(FakeTarget {
            observation: Mutex::new(SupabaseObservation {
                exposed: BTreeSet::from(["public".into()]),
                ..SupabaseObservation::default()
            }),
            sql: sql.clone(),
            applies: Mutex::new(0),
            retired: Mutex::new(Vec::new()),
        });
        let slice = slice(&sql);
        let report = controller
            .execute(&ControllerCommand::Reconcile(slice.clone()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.dispositions().len(), 1);
        assert_eq!(report.outputs().len(), 7);
        controller
            .execute(&ControllerCommand::Reconcile(slice.clone()))
            .await
            .unwrap();
        assert_eq!(*controller.target.applies.lock().unwrap(), 1);
        controller
            .execute(&ControllerCommand::Retire(Retirement {
                graph_id: slice.graph_id(),
                last_generation: slice.generation(),
                controller: controller.name().clone(),
                resources: vec![slice.resources()[0].id()],
            }))
            .await
            .unwrap();
        assert_eq!(
            controller.target.retired.lock().unwrap().as_slice(),
            &[slice.resources()[0].id()]
        );
    }

    #[test]
    fn migration_mutation_diagnostic_is_stable() {
        let sql = "select 1;";
        let slice = slice(sql);
        let resource = &slice.resources()[0];
        let observed = SupabaseObservation {
            migrations: BTreeMap::from([((resource.id(), "001".into()), "sha256:old".into())]),
            exposed: BTreeSet::from(["public".into()]),
            ..SupabaseObservation::default()
        };
        let target = FakeTarget {
            observation: Mutex::new(observed.clone()),
            sql: sql.into(),
            applies: Mutex::new(0),
            retired: Mutex::new(Vec::new()),
        };
        let error = build_plan(&slice, &observed, &target).unwrap_err();
        insta::assert_snapshot!(error.to_string(), @r###"
        error[supabase.plan.migration-mutated]: migration "001" for catalog/supabase/schema@1/catalog was applied with sha256:old, but desired declares sha256:354b7196c9ba5fb4b21cf615bb6ec4cd5c07503c34229feef033fc081a8c03f4
          --> migrations/001.sql
          = help: never edit an applied migration ID; append a corrective migration
        "###);
    }

    fn slice(sql: &str) -> ControllerSlice {
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
            path: ResourcePath::new(ComponentName::new("catalog").unwrap(), ResourceAddress::new(KindVersion::new(KindName::new("supabase/schema").unwrap(), NonZeroU32::new(1).unwrap()), ResourceName::new("catalog").unwrap())),
            controller: controller_name(CONTROLLER_NAME),
            body: serde_json::json!({"stack":"local","project":"henosis-local","database":"postgres","schema":"catalog","migrations":[{"id":"001","path":"migrations/001.sql","sha256":checksum}],"api":{"expose":true,"anonAccess":"read"}}).try_into().unwrap(),
            outputs,
        }).unwrap();
        ControllerSlice::new(
            GraphId::from_bytes([3; 16]),
            Generation::new(1).unwrap(),
            ContentDigest::digest(b"plan"),
            controller_name(CONTROLLER_NAME),
            vec![resource],
            Vec::new(),
        )
    }
}
