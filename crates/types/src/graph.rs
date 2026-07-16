use std::collections::BTreeMap;
use std::collections::BTreeSet;

use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::ComponentName;
use crate::ContentDigest;
use crate::Generation;
use crate::GraphId;
use crate::InputName;
use crate::NativeValue;
use crate::OutputAvailability;
use crate::OutputName;
use crate::OutputRef;
use crate::ValueSchema;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum GraphSourcePolicy {
    #[default]
    AcceptLocal,
    RequireVcs,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SourceProvenance {
    Local {
        repository: Option<String>,
        base_revision: Option<String>,
        dirty: bool,
    },
    Vcs {
        repository: String,
        revision: String,
        reference: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BundleRef(ContentDigest);

impl BundleRef {
    #[must_use]
    pub const fn new(digest: ContentDigest) -> Self {
        Self(digest)
    }

    #[must_use]
    pub const fn digest(self) -> ContentDigest {
        self.0
    }
}

impl std::fmt::Display for BundleRef {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ComponentInputSource {
    Output {
        source: OutputRef,
        optional: bool,
    },
    Config {
        schema: ValueSchema,
        default: Option<NativeValue>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComponentInput {
    name: InputName,
    source: ComponentInputSource,
}

impl ComponentInput {
    #[must_use]
    pub const fn new(name: InputName, source: OutputRef, optional: bool) -> Self {
        Self {
            name,
            source: ComponentInputSource::Output { source, optional },
        }
    }

    #[must_use]
    pub const fn config(
        name: InputName,
        schema: ValueSchema,
        default: Option<NativeValue>,
    ) -> Self {
        Self {
            name,
            source: ComponentInputSource::Config { schema, default },
        }
    }

    #[must_use]
    pub const fn name(&self) -> &InputName {
        &self.name
    }

    #[must_use]
    pub const fn source(&self) -> &ComponentInputSource {
        &self.source
    }

    #[must_use]
    pub const fn output_source(&self) -> Option<&OutputRef> {
        match &self.source {
            ComponentInputSource::Output { source, .. } => Some(source),
            ComponentInputSource::Config { .. } => None,
        }
    }

    #[must_use]
    pub const fn is_optional(&self) -> bool {
        matches!(
            self.source,
            ComponentInputSource::Output { optional: true, .. }
        )
    }

    #[must_use]
    pub const fn config_schema(&self) -> Option<&ValueSchema> {
        match &self.source {
            ComponentInputSource::Config { schema, .. } => Some(schema),
            ComponentInputSource::Output { .. } => None,
        }
    }

    #[must_use]
    pub const fn config_default(&self) -> Option<&NativeValue> {
        match &self.source {
            ComponentInputSource::Config { default, .. } => default.as_ref(),
            ComponentInputSource::Output { .. } => None,
        }
    }
}

impl IdOrdItem for ComponentInput {
    type Key<'a> = &'a InputName;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        &self.name
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ComponentRevision(String);

impl ComponentRevision {
    pub fn new(value: impl Into<String>) -> Result<Self, ComponentRevisionError> {
        let value = value.into();
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ComponentRevisionError);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn short(&self) -> &str {
        &self.0[..12]
    }
}

impl std::fmt::Display for ComponentRevision {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("component revision must be a lowercase 64-character SHA-256 digest")]
pub struct ComponentRevisionError;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompiledOutputContract {
    availability: OutputAvailability,
    optional: bool,
    schema: ValueSchema,
}

impl CompiledOutputContract {
    #[must_use]
    pub const fn new(
        availability: OutputAvailability,
        optional: bool,
        schema: ValueSchema,
    ) -> Self {
        Self {
            availability,
            optional,
            schema,
        }
    }

    #[must_use]
    pub const fn availability(&self) -> OutputAvailability {
        self.availability
    }

    #[must_use]
    pub const fn is_optional(&self) -> bool {
        self.optional
    }

    #[must_use]
    pub const fn schema(&self) -> &ValueSchema {
        &self.schema
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompiledDependency {
    component: ComponentName,
    revision: ComponentRevision,
    outputs: BTreeMap<OutputName, CompiledOutputContract>,
    consumed_outputs: BTreeSet<OutputName>,
}

impl CompiledDependency {
    #[must_use]
    pub fn new(
        component: ComponentName,
        revision: ComponentRevision,
        outputs: BTreeMap<OutputName, CompiledOutputContract>,
        consumed_outputs: BTreeSet<OutputName>,
    ) -> Self {
        Self {
            component,
            revision,
            outputs,
            consumed_outputs,
        }
    }

    #[must_use]
    pub const fn component(&self) -> &ComponentName {
        &self.component
    }

    #[must_use]
    pub const fn revision(&self) -> &ComponentRevision {
        &self.revision
    }

    pub fn outputs(&self) -> impl ExactSizeIterator<Item = (&OutputName, &CompiledOutputContract)> {
        self.outputs.iter()
    }

    #[must_use]
    pub fn output(&self, name: &OutputName) -> Option<&CompiledOutputContract> {
        self.outputs.get(name)
    }

    pub fn consumed_outputs(&self) -> impl ExactSizeIterator<Item = &OutputName> {
        self.consumed_outputs.iter()
    }
}

impl IdOrdItem for CompiledDependency {
    type Key<'a> = &'a ComponentName;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        &self.component
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComponentOutput {
    name: OutputName,
    availability: OutputAvailability,
    optional: bool,
    schema: ValueSchema,
}

impl ComponentOutput {
    #[must_use]
    pub const fn new(
        name: OutputName,
        availability: OutputAvailability,
        optional: bool,
        schema: ValueSchema,
    ) -> Self {
        Self {
            name,
            availability,
            optional,
            schema,
        }
    }

    #[must_use]
    pub const fn name(&self) -> &OutputName {
        &self.name
    }

    #[must_use]
    pub const fn availability(&self) -> OutputAvailability {
        self.availability
    }

    #[must_use]
    pub const fn is_optional(&self) -> bool {
        self.optional
    }

    #[must_use]
    pub const fn schema(&self) -> &ValueSchema {
        &self.schema
    }
}

impl IdOrdItem for ComponentOutput {
    type Key<'a> = &'a OutputName;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        &self.name
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComponentInputBinding {
    name: InputName,
    value: NativeValue,
}

impl ComponentInputBinding {
    #[must_use]
    pub const fn new(name: InputName, value: NativeValue) -> Self {
        Self { name, value }
    }

    #[must_use]
    pub const fn name(&self) -> &InputName {
        &self.name
    }

    #[must_use]
    pub const fn value(&self) -> &NativeValue {
        &self.value
    }
}

impl IdOrdItem for ComponentInputBinding {
    type Key<'a> = &'a InputName;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        &self.name
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NewComponentIntent {
    pub name: ComponentName,
    pub revision: ComponentRevision,
    pub bundle: BundleRef,
    pub inputs: Vec<ComponentInput>,
    pub outputs: Vec<ComponentOutput>,
    pub compiled_dependencies: Vec<CompiledDependency>,
    pub source: Option<SourceProvenance>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComponentIntent {
    name: ComponentName,
    revision: ComponentRevision,
    bundle: BundleRef,
    inputs: IdOrdMap<ComponentInput>,
    input_bindings: IdOrdMap<ComponentInputBinding>,
    outputs: IdOrdMap<ComponentOutput>,
    compiled_dependencies: IdOrdMap<CompiledDependency>,
    source: Option<SourceProvenance>,
}

impl ComponentIntent {
    pub fn new(new: NewComponentIntent) -> Result<Self, ComponentIntentError> {
        let mut inputs = IdOrdMap::with_capacity(new.inputs.len());
        for input in new.inputs {
            inputs
                .insert_unique(input)
                .map_err(|_| ComponentIntentError::DuplicateInput)?;
        }
        let mut outputs = IdOrdMap::with_capacity(new.outputs.len());
        for output in new.outputs {
            outputs
                .insert_unique(output)
                .map_err(|_| ComponentIntentError::DuplicateOutput)?;
        }
        let mut compiled_dependencies = IdOrdMap::with_capacity(new.compiled_dependencies.len());
        for dependency in new.compiled_dependencies {
            compiled_dependencies
                .insert_unique(dependency)
                .map_err(|_| ComponentIntentError::DuplicateCompiledDependency)?;
        }
        Ok(Self {
            name: new.name,
            revision: new.revision,
            bundle: new.bundle,
            inputs,
            input_bindings: IdOrdMap::new(),
            outputs,
            compiled_dependencies,
            source: new.source,
        })
    }

    #[must_use]
    pub const fn name(&self) -> &ComponentName {
        &self.name
    }

    #[must_use]
    pub const fn revision(&self) -> &ComponentRevision {
        &self.revision
    }

    #[must_use]
    pub const fn bundle(&self) -> BundleRef {
        self.bundle
    }

    pub fn inputs(&self) -> impl ExactSizeIterator<Item = &ComponentInput> {
        self.inputs.iter()
    }

    #[must_use]
    pub fn input(&self, name: &InputName) -> Option<&ComponentInput> {
        self.inputs.get(name)
    }

    pub fn input_bindings(&self) -> impl ExactSizeIterator<Item = &ComponentInputBinding> {
        self.input_bindings.iter()
    }

    #[must_use]
    pub fn input_binding(&self, name: &InputName) -> Option<&ComponentInputBinding> {
        self.input_bindings.get(name)
    }

    pub fn with_input_bindings(
        mut self,
        bindings: Vec<ComponentInputBinding>,
    ) -> Result<Self, ComponentIntentError> {
        let mut keyed = IdOrdMap::with_capacity(bindings.len());
        for binding in bindings {
            keyed
                .insert_unique(binding)
                .map_err(|_| ComponentIntentError::DuplicateInputBinding)?;
        }
        self.input_bindings = keyed;
        Ok(self)
    }

    pub fn outputs(&self) -> impl ExactSizeIterator<Item = &ComponentOutput> {
        self.outputs.iter()
    }

    #[must_use]
    pub fn output(&self, name: &OutputName) -> Option<&ComponentOutput> {
        self.outputs.get(name)
    }

    pub fn compiled_dependencies(&self) -> impl ExactSizeIterator<Item = &CompiledDependency> {
        self.compiled_dependencies.iter()
    }

    #[must_use]
    pub fn compiled_dependency(&self, name: &ComponentName) -> Option<&CompiledDependency> {
        self.compiled_dependencies.get(name)
    }

    #[must_use]
    pub const fn source(&self) -> Option<&SourceProvenance> {
        self.source.as_ref()
    }

    #[must_use]
    pub fn with_source(mut self, source: Option<SourceProvenance>) -> Self {
        self.source = source;
        self
    }
}

impl IdOrdItem for ComponentIntent {
    type Key<'a> = &'a ComponentName;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        &self.name
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ComponentIntentError {
    #[error("component declares an input name more than once")]
    DuplicateInput,
    #[error("component declares an output name more than once")]
    DuplicateOutput,
    #[error("component binds an input name more than once")]
    DuplicateInputBinding,
    #[error("component carries compiled contract facts for a producer more than once")]
    DuplicateCompiledDependency,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NewGraphIntent {
    pub id: GraphId,
    pub components: Vec<ComponentIntent>,
    pub source_policy: GraphSourcePolicy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GraphIntent {
    id: GraphId,
    generation: Generation,
    components: IdOrdMap<ComponentIntent>,
    source_policy: GraphSourcePolicy,
}

impl GraphIntent {
    pub fn new(new: NewGraphIntent) -> Result<Self, GraphIntentError> {
        Self::from_parts(
            new.id,
            Generation::new(1).expect("one is a valid generation"),
            new.components,
            new.source_policy,
        )
    }

    pub fn replace_components(
        &self,
        components: Vec<ComponentIntent>,
    ) -> Result<Self, GraphIntentError> {
        Self::from_parts(
            self.id,
            self.generation.next(),
            components,
            self.source_policy,
        )
    }

    fn from_parts(
        id: GraphId,
        generation: Generation,
        components: Vec<ComponentIntent>,
        source_policy: GraphSourcePolicy,
    ) -> Result<Self, GraphIntentError> {
        if components.is_empty() {
            return Err(GraphIntentError::Empty);
        }
        let mut keyed = IdOrdMap::with_capacity(components.len());
        for component in components {
            keyed
                .insert_unique(component)
                .map_err(|_| GraphIntentError::DuplicateComponent)?;
        }
        if source_policy == GraphSourcePolicy::RequireVcs {
            let local = keyed
                .iter()
                .filter(|component| {
                    !matches!(component.source(), Some(SourceProvenance::Vcs { .. }))
                })
                .map(|component| component.name().to_string())
                .collect::<Vec<_>>();
            if !local.is_empty() {
                return Err(GraphIntentError::VcsRequired(local.join(", ")));
            }
        }
        let mut contract_diagnostics = Vec::new();
        let mut config_diagnostics = Vec::new();
        for component in &keyed {
            for dependency in component.compiled_dependencies() {
                if let Some(diagnostic) =
                    contract_diagnostic(component, dependency, keyed.get(dependency.component()))
                {
                    contract_diagnostics.push(diagnostic);
                }
            }
            for input in component.inputs() {
                let ComponentInputSource::Output { source, .. } = input.source() else {
                    continue;
                };
                let carried = component
                    .compiled_dependency(source.component())
                    .is_some_and(|dependency| {
                        dependency.consumed_outputs.contains(source.output())
                    });
                if !carried {
                    contract_diagnostics.push(format!(
                        "error[HENOSIS_CONTRACT_FACTS_MISSING]: component {:?} consumes {} but \
                         its bundle carries no compiled-against contract for that edge\n  --> {} \
                         -> {}\n  = help: rebuild the consumer with a Henosis bundler that \
                         records resolved dependency contracts",
                        component.name().as_str(),
                        source,
                        component.name(),
                        source,
                    ));
                }
            }
            for input in component.inputs() {
                match input.source() {
                    ComponentInputSource::Output { .. } => {}
                    ComponentInputSource::Config { schema, default } => {
                        let value = component
                            .input_binding(input.name())
                            .map(ComponentInputBinding::value)
                            .or(default.as_ref());
                        match value {
                            None => config_diagnostics.push(format!(
                                "component {:?} input {:?}: missing required binding (expected \
                                 {schema})",
                                component.name().as_str(),
                                input.name().as_str()
                            )),
                            Some(value) if !schema.accepts(value.as_json()) => {
                                config_diagnostics.push(format!(
                                    "component {:?} input {:?}: expected {schema}, received {}",
                                    component.name().as_str(),
                                    input.name().as_str(),
                                    json_kind(value.as_json())
                                ));
                            }
                            Some(_) => {}
                        }
                    }
                }
            }
            for binding in component.input_bindings() {
                match component.input(binding.name()) {
                    None => config_diagnostics.push(format!(
                        "component {:?} input {:?}: binding names no declared input",
                        component.name().as_str(),
                        binding.name().as_str()
                    )),
                    Some(input) if input.config_schema().is_none() => {
                        config_diagnostics.push(format!(
                            "component {:?} input {:?}: output-sourced inputs cannot be bound by \
                             the graph",
                            component.name().as_str(),
                            binding.name().as_str()
                        ));
                    }
                    Some(_) => {}
                }
            }
        }
        if !contract_diagnostics.is_empty() {
            contract_diagnostics.sort();
            contract_diagnostics.dedup();
            return Err(GraphIntentError::InvalidContracts(
                contract_diagnostics.join("\n\n"),
            ));
        }
        if !config_diagnostics.is_empty() {
            return Err(GraphIntentError::InvalidConfigBindings(
                config_diagnostics.join("\n  - "),
            ));
        }
        Ok(Self {
            id,
            generation,
            components: keyed,
            source_policy,
        })
    }

    #[must_use]
    pub const fn id(&self) -> GraphId {
        self.id
    }

    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.generation
    }

    pub fn components(&self) -> impl ExactSizeIterator<Item = &ComponentIntent> {
        self.components.iter()
    }

    #[must_use]
    pub fn component(&self, name: &ComponentName) -> Option<&ComponentIntent> {
        self.components.get(name)
    }

    #[must_use]
    pub const fn source_policy(&self) -> GraphSourcePolicy {
        self.source_policy
    }
}

impl IdOrdItem for GraphIntent {
    type Key<'a> = GraphId;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.id
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum GraphIntentError {
    #[error("graph intent must contain at least one component")]
    Empty,
    #[error("graph intent contains a component name more than once")]
    DuplicateComponent,
    #[error("graph source policy requires VCS provenance for components: {0}")]
    VcsRequired(String),
    #[error("graph intent has incompatible component contracts:\n{0}")]
    InvalidContracts(String),
    #[error("graph intent has invalid config input bindings:\n  - {0}")]
    InvalidConfigBindings(String),
}

fn contract_diagnostic(
    consumer: &ComponentIntent,
    dependency: &CompiledDependency,
    producer: Option<&ComponentIntent>,
) -> Option<String> {
    let mut breaks = Vec::new();
    for output_name in dependency.consumed_outputs() {
        let expected = dependency
            .output(output_name)
            .expect("bundle metadata validates consumed outputs against its full schema");
        match producer.and_then(|producer| producer.output(output_name)) {
            None => breaks.push((output_name, expected, None, "removed".to_owned())),
            Some(actual) if actual.schema() != expected.schema() => breaks.push((
                output_name,
                expected,
                Some(actual),
                format!(
                    "{} -> {}",
                    contract_schema(expected.schema()),
                    contract_schema(actual.schema())
                ),
            )),
            Some(actual) if actual.is_optional() != expected.is_optional() => breaks.push((
                output_name,
                expected,
                Some(actual),
                format!(
                    "{} -> {}",
                    optionality(expected.is_optional()),
                    optionality(actual.is_optional())
                ),
            )),
            Some(_) => {}
        }
    }
    if breaks.is_empty() {
        return None;
    }

    let resolved_revision = producer
        .map(ComponentIntent::revision)
        .map(ComponentRevision::short)
        .unwrap_or("missing");
    let paths = breaks
        .iter()
        .map(|(name, _, _, fate)| format!("{name} ({fate})"))
        .collect::<Vec<_>>()
        .join(", ");
    let summary = if breaks.len() == 1 {
        let (name, expected, actual, _) = &breaks[0];
        match actual {
            None => format!(
                "{} compiled against {}@{} where {}: {}; this graph pins {}@{} which does not \
                 declare {}",
                consumer.name(),
                dependency.component(),
                dependency.revision().short(),
                name,
                contract_schema(expected.schema()),
                dependency.component(),
                resolved_revision,
                name,
            ),
            Some(actual) => format!(
                "{} compiled against {}@{} where {}: {}; this graph pins {}@{} where {}: {}",
                consumer.name(),
                dependency.component(),
                dependency.revision().short(),
                name,
                contract_schema(expected.schema()),
                dependency.component(),
                resolved_revision,
                name,
                contract_schema(actual.schema()),
            ),
        }
    } else {
        format!(
            "{} compiled against {}@{}, but this graph pins incompatible {}@{}",
            consumer.name(),
            dependency.component(),
            dependency.revision().short(),
            dependency.component(),
            resolved_revision,
        )
    };
    let expected_schema = render_compiled_schema(dependency.outputs());
    let actual_schema = producer
        .map(|producer| {
            let rendered =
                render_component_schema(producer.outputs().map(|output| (output.name(), output)));
            if rendered.is_empty() {
                "<no outputs>".to_owned()
            } else {
                rendered
            }
        })
        .unwrap_or_else(|| "<component missing>".to_owned());
    Some(format!(
        "error[HENOSIS_CONTRACT_SKEW]: {summary}\n  --> {} -> {}: {paths}\n   |\n   | \
         compiled-against outputs\n{}\n   | resolved outputs\n{}\n   |\n  = note: {} was built \
         against {}@{}; the graph resolves {}@{}\n  = help: update {} to the resolved producer \
         contract, or pin {} to the revision {} was built against",
        consumer.name(),
        dependency.component(),
        indent_schema(&expected_schema, '-'),
        indent_schema(&actual_schema, '+'),
        consumer.name(),
        dependency.component(),
        dependency.revision().short(),
        dependency.component(),
        resolved_revision,
        consumer.name(),
        dependency.component(),
        consumer.name(),
    ))
}

fn render_compiled_schema<'a>(
    outputs: impl Iterator<Item = (&'a OutputName, &'a CompiledOutputContract)>,
) -> String {
    outputs
        .map(|(name, output)| {
            format!(
                "{name}: {}{}",
                contract_schema(output.schema()),
                if output.is_optional() { "?" } else { "" }
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_component_schema<'a>(
    outputs: impl Iterator<Item = (&'a OutputName, &'a ComponentOutput)>,
) -> String {
    outputs
        .map(|(name, output)| {
            format!(
                "{name}: {}{}",
                contract_schema(output.schema()),
                if output.is_optional() { "?" } else { "" }
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn indent_schema(schema: &str, marker: char) -> String {
    schema
        .lines()
        .map(|line| format!("   {marker} {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn optionality(optional: bool) -> &'static str {
    if optional { "optional" } else { "required" }
}

fn contract_schema(schema: &ValueSchema) -> String {
    match schema {
        ValueSchema::String => "string".to_owned(),
        ValueSchema::Url => "url".to_owned(),
        ValueSchema::Number => "number".to_owned(),
        ValueSchema::Boolean => "boolean".to_owned(),
        ValueSchema::Json => "json".to_owned(),
        ValueSchema::Artifact => "artifact".to_owned(),
        ValueSchema::Array { element } => format!("{}[]", contract_schema(element)),
        ValueSchema::Object { fields } => format!(
            "{{ {} }}",
            fields
                .iter()
                .map(|(name, schema)| format!("{name}: {}", contract_schema(schema)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn json_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}
