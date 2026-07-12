use crate::RegisteredComponentSpec;

/// Registers immutable component input under its content hash.
#[derive(Clone, Debug)]
pub struct RegisterComponentSpec {
    component: RegisteredComponentSpec,
}

impl RegisterComponentSpec {
    #[must_use]
    pub const fn new(component: RegisteredComponentSpec) -> Self {
        Self { component }
    }

    #[must_use]
    pub const fn component(&self) -> &RegisteredComponentSpec {
        &self.component
    }

    #[must_use]
    pub fn into_component(self) -> RegisteredComponentSpec {
        self.component
    }
}
