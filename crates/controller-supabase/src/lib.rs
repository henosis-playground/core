//! Local Supabase schema controller.
//!
//! Reconciliation keeps the proven plan/apply split: desired resources and fresh target receipts
//! produce an immutable ordered plan, and only that plan is handed to the target. Applied migration
//! IDs are immutable and checksummed. The target boundary resolves repository-relative migration
//! files because `supabase/schema@1` carries path plus digest, not SQL bytes.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use futures::FutureExt as _;
use futures::future::BoxFuture;
use henosis_controller_runtime::{
    controller_name, failed_report, output, publication_id, ready_report,
};
use henosis_types::{
    Controller, ControllerCommand, ControllerError, ControllerName, ControllerReport,
    ControllerSlice, GraphId, Resource, ResourceId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
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
                ("databaseUrlRef", serde_json::json!(self.target.database_url_ref())),
                ("anonKeyRef", serde_json::json!(self.target.anon_key_ref())),
            ] {
                if resource.outputs().any(|declaration| declaration.name().as_str() == name) {
                    outputs.push(output(resource, name, value).map_err(|error| {
                        SupabaseError::Plan(error.to_string())
                    })?);
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
        let body: SchemaBody = serde_json::from_value(resource.body().as_json().clone())
            .map_err(|error| {
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
                        "error[supabase.plan.migration-mutated]: migration {:?} for {} was applied with {}, but desired declares {}\n  --> {}\n  = help: never edit an applied migration ID; append a corrective migration",
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
                    "error[supabase.plan.checksum]: migration {:?} for {} declares {}, but file {} hashes to {}\n  = help: regenerate the migration reference from the exact committed SQL bytes",
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
        operations.push(SupabaseOperation::ConfigureApi {
            exposed,
            anon_read,
        });
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
            "error[supabase.schema.invalid]: {} schema {:?} must contain lowercase letters, digits, or underscores",
            resource.path(), body.schema
        )));
    }
    let mut ids = BTreeSet::new();
    for migration in &body.migrations {
        if !ids.insert(&migration.id) {
            return Err(SupabaseError::Plan(format!(
                "error[supabase.migration.duplicate]: {} repeats migration ID {:?}",
                resource.path(), migration.id
            )));
        }
        if migration.path.starts_with('/')
            || migration.path.split(['/', '\\']).any(|part| part == "..")
        {
            return Err(SupabaseError::Plan(format!(
                "error[supabase.migration.path]: {} migration {:?} path must be repository-relative without parent traversal",
                resource.path(), migration.id
            )));
        }
        if !migration.sha256.starts_with("sha256:") || migration.sha256.len() != 71 {
            return Err(SupabaseError::Plan(format!(
                "error[supabase.migration.checksum]: {} migration {:?} needs a lowercase SHA-256 digest",
                resource.path(), migration.id
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

    use henosis_types::{
        ComponentName, ContentDigest, ControllerSlice, Generation, KindName, KindVersion,
        NewResource, OutputAvailability, OutputDeclaration, OutputName, ResourceAddress,
        ResourceName, ResourcePath, Retirement,
    };

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
                    SupabaseOperation::EnsureSchema { schema, .. } => { observed.schemas.insert(schema.clone()); }
                    SupabaseOperation::ApplyMigration { resource, id, checksum, .. } => { observed.migrations.insert((*resource, id.clone()), checksum.clone()); }
                    SupabaseOperation::ConfigureApi { exposed, anon_read } => { observed.exposed = exposed.clone(); observed.anon_read = anon_read.clone(); }
                }
            }
            Ok(observation_digest(&observed))
        }
        fn retire(&self, _graph: GraphId, resources: &[ResourceId]) -> Result<(), SupabaseError> {
            self.retired.lock().unwrap().extend_from_slice(resources);
            Ok(())
        }
        fn api_url(&self) -> &str { "http://localhost:4484" }
        fn database_url_ref(&self) -> &str { "docker-secret://supabase-connection-url" }
        fn anon_key_ref(&self) -> &str { "docker-secret://supabase-anon-key" }
    }

    #[tokio::test]
    async fn plans_applies_reports_idempotently_and_retires() {
        let sql = "create table items (id bigint primary key);".to_owned();
        let controller = SupabaseController::new(FakeTarget {
            observation: Mutex::new(SupabaseObservation { exposed: BTreeSet::from(["public".into()]), ..SupabaseObservation::default() }),
            sql: sql.clone(),
            applies: Mutex::new(0),
            retired: Mutex::new(Vec::new()),
        });
        let slice = slice(&sql);
        let report = controller.execute(&ControllerCommand::Reconcile(slice.clone())).await.unwrap().unwrap();
        assert_eq!(report.dispositions().len(), 1);
        assert_eq!(report.outputs().len(), 7);
        controller.execute(&ControllerCommand::Reconcile(slice.clone())).await.unwrap();
        assert_eq!(*controller.target.applies.lock().unwrap(), 1);
        controller.execute(&ControllerCommand::Retire(Retirement {
            graph_id: slice.graph_id(), last_generation: slice.generation(), controller: controller.name().clone(), resources: vec![slice.resources()[0].id()],
        })).await.unwrap();
        assert_eq!(controller.target.retired.lock().unwrap().as_slice(), &[slice.resources()[0].id()]);
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
        let target = FakeTarget { observation: Mutex::new(observed.clone()), sql: sql.into(), applies: Mutex::new(0), retired: Mutex::new(Vec::new()) };
        let error = build_plan(&slice, &observed, &target).unwrap_err();
        insta::assert_snapshot!(error.to_string(), @r###"
        error[supabase.plan.migration-mutated]: migration "001" for catalog/supabase/schema@1/catalog was applied with sha256:old, but desired declares sha256:354b7196c9ba5fb4b21cf615bb6ec4cd5c07503c34229feef033fc081a8c03f4
          --> migrations/001.sql
          = help: never edit an applied migration ID; append a corrective migration
        "###);
    }

    fn slice(sql: &str) -> ControllerSlice {
        let checksum = format!("sha256:{}", hex::encode(Sha256::digest(sql.as_bytes())));
        let outputs = ["project", "database", "schema", "apiUrl", "restUrl", "databaseUrlRef", "anonKeyRef"].into_iter().map(|name| OutputDeclaration::new(OutputName::new(name).unwrap(), OutputAvailability::Observed)).collect();
        let resource = Resource::new(NewResource {
            id: ResourceId::from_bytes([4; 16]),
            path: ResourcePath::new(ComponentName::new("catalog").unwrap(), ResourceAddress::new(KindVersion::new(KindName::new("supabase/schema").unwrap(), NonZeroU32::new(1).unwrap()), ResourceName::new("catalog").unwrap())),
            controller: controller_name(CONTROLLER_NAME),
            body: serde_json::json!({"stack":"local","project":"henosis-local","database":"postgres","schema":"catalog","migrations":[{"id":"001","path":"migrations/001.sql","sha256":checksum}],"api":{"expose":true,"anonAccess":"read"}}).try_into().unwrap(),
            outputs,
        }).unwrap();
        ControllerSlice::new(GraphId::from_bytes([3; 16]), Generation::new(1).unwrap(), ContentDigest::digest(b"plan"), controller_name(CONTROLLER_NAME), vec![resource], Vec::new())
    }
}
