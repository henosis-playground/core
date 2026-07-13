use crate::domain::ComponentUuid;
use crate::domain::NewComponentSpec;

/// A component and its initial specification awaiting durable acceptance.
#[derive(Clone, Debug)]
pub struct NewComponent {
    id: ComponentUuid,
    spec: NewComponentSpec,
}

impl NewComponent {
    #[must_use]
    pub const fn new(id: ComponentUuid, spec: NewComponentSpec) -> Self {
        Self { id, spec }
    }

    #[must_use]
    pub const fn id(&self) -> ComponentUuid {
        self.id
    }

    #[must_use]
    pub const fn spec(&self) -> &NewComponentSpec {
        &self.spec
    }

    #[must_use]
    pub fn into_spec(self) -> NewComponentSpec {
        self.spec
    }
}
