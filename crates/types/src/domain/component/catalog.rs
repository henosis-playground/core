use iddqd::IdOrdMap;
use thiserror::Error;

use crate::domain::Component;
use crate::domain::ComponentUuid;

/// Latest accepted generation of every known component.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ComponentCatalog {
    components: IdOrdMap<Component>,
}

/// Failure while applying a component to the catalog.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ComponentCatalogError {
    #[error("component references an unregistered dependency")]
    MissingDependency,
    #[error("component generation did not advance")]
    StaleGeneration,
}

impl ComponentCatalog {
    pub fn apply(&mut self, component: Component) -> Result<(), ComponentCatalogError> {
        if component
            .spec()
            .depends_on()
            .iter()
            .any(|dependency| !self.components.contains_key(dependency))
        {
            return Err(ComponentCatalogError::MissingDependency);
        }
        if let Some(stored) = self.components.get(&component.id())
            && component.generation() <= stored.generation()
        {
            return Err(ComponentCatalogError::StaleGeneration);
        }
        self.components.insert_overwrite(component);
        Ok(())
    }

    #[must_use]
    pub fn get(&self, id: ComponentUuid) -> Option<&Component> {
        self.components.get(&id)
    }

    #[must_use]
    pub fn contains(&self, id: ComponentUuid) -> bool {
        self.components.contains_key(&id)
    }

    #[must_use]
    pub fn missing<'a>(
        &'a self,
        ids: impl IntoIterator<Item = &'a ComponentUuid>,
    ) -> Vec<ComponentUuid> {
        ids.into_iter()
            .copied()
            .filter(|id| !self.contains(*id))
            .collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Component> {
        self.components.iter()
    }
}
