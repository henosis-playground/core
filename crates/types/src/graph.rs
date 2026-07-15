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
use crate::GraphName;
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComponentOutput {
    name: OutputName,
    availability: OutputAvailability,
    optional: bool,
}

impl ComponentOutput {
    #[must_use]
    pub const fn new(name: OutputName, availability: OutputAvailability, optional: bool) -> Self {
        Self {
            name,
            availability,
            optional,
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
    pub bundle: BundleRef,
    pub inputs: Vec<ComponentInput>,
    pub outputs: Vec<ComponentOutput>,
    pub source: Option<SourceProvenance>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComponentIntent {
    name: ComponentName,
    bundle: BundleRef,
    inputs: IdOrdMap<ComponentInput>,
    input_bindings: IdOrdMap<ComponentInputBinding>,
    outputs: IdOrdMap<ComponentOutput>,
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
        Ok(Self {
            name: new.name,
            bundle: new.bundle,
            inputs,
            input_bindings: IdOrdMap::new(),
            outputs,
            source: new.source,
        })
    }

    #[must_use]
    pub const fn name(&self) -> &ComponentName {
        &self.name
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
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NewGraphIntent {
    pub id: GraphId,
    pub name: GraphName,
    pub components: Vec<ComponentIntent>,
    pub source_policy: GraphSourcePolicy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GraphIntent {
    id: GraphId,
    name: GraphName,
    generation: Generation,
    components: IdOrdMap<ComponentIntent>,
    source_policy: GraphSourcePolicy,
}

impl GraphIntent {
    pub fn new(new: NewGraphIntent) -> Result<Self, GraphIntentError> {
        Self::from_parts(
            new.id,
            new.name,
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
            self.name.clone(),
            self.generation.next(),
            components,
            self.source_policy,
        )
    }

    fn from_parts(
        id: GraphId,
        name: GraphName,
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
        let mut config_diagnostics = Vec::new();
        for component in &keyed {
            for input in component.inputs() {
                match input.source() {
                    ComponentInputSource::Output { source, optional } => {
                        let producer = keyed
                            .get(source.component())
                            .ok_or(GraphIntentError::UnknownInputComponent)?;
                        let output = producer
                            .output(source.output())
                            .ok_or(GraphIntentError::UnknownInputOutput)?;
                        if output.is_optional() && !optional {
                            return Err(GraphIntentError::RequiredInputFromOptionalOutput);
                        }
                    }
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
        if !config_diagnostics.is_empty() {
            return Err(GraphIntentError::InvalidConfigBindings(
                config_diagnostics.join("\n  - "),
            ));
        }
        Ok(Self {
            id,
            name,
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
    pub const fn name(&self) -> &GraphName {
        &self.name
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
    #[error("component input refers to an unknown producer component")]
    UnknownInputComponent,
    #[error("component input refers to an unknown producer output")]
    UnknownInputOutput,
    #[error("a required input cannot consume an optional producer output")]
    RequiredInputFromOptionalOutput,
    #[error("graph source policy requires VCS provenance for components: {0}")]
    VcsRequired(String),
    #[error("graph intent has invalid config input bindings:\n  - {0}")]
    InvalidConfigBindings(String),
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
