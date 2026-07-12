use iddqd::IdOrdMap;
use thiserror::Error;

use crate::ComponentSpecHash;
use crate::RegisteredComponentSpec;

/// Immutable component specs indexed by content hash.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SpecCatalog {
    specs: IdOrdMap<RegisteredComponentSpec>,
}

/// Failure while applying a component spec to the catalog.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SpecCatalogError {
    #[error("component spec hash is already registered with different content")]
    HashCollision,
    #[error("component spec references an unregistered dependency")]
    MissingDependency,
}

impl SpecCatalog {
    pub fn apply(&mut self, registered: RegisteredComponentSpec) -> Result<(), SpecCatalogError> {
        if registered
            .spec()
            .depends_on()
            .iter()
            .any(|dependency| !self.specs.contains_key(dependency))
        {
            return Err(SpecCatalogError::MissingDependency);
        }
        match self.specs.get(&registered.hash()) {
            Some(stored) if stored.spec() != registered.spec() => {
                Err(SpecCatalogError::HashCollision)
            }
            Some(_) => Ok(()),
            None => {
                self.specs
                    .insert_unique(registered)
                    .expect("absence was checked before insertion");
                Ok(())
            }
        }
    }

    #[must_use]
    pub fn get(&self, hash: ComponentSpecHash) -> Option<&RegisteredComponentSpec> {
        self.specs.get(&hash)
    }

    #[must_use]
    pub fn contains(&self, hash: ComponentSpecHash) -> bool {
        self.specs.contains_key(&hash)
    }

    #[must_use]
    pub fn missing<'a>(
        &'a self,
        hashes: impl IntoIterator<Item = &'a ComponentSpecHash>,
    ) -> Vec<ComponentSpecHash> {
        hashes
            .into_iter()
            .copied()
            .filter(|hash| !self.contains(*hash))
            .collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = &RegisteredComponentSpec> {
        self.specs.iter()
    }
}
