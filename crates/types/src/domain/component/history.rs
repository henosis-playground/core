use std::time::SystemTime;

use thiserror::Error;

use crate::domain::Component;
use crate::domain::ComponentUuid;
use crate::domain::NewComponentSpec;

/// One accepted component-specification record with its S2 metadata.
#[derive(Clone, Debug)]
pub struct ComponentSpecEvent {
    component_id: ComponentUuid,
    generation: u64,
    time_recorded: SystemTime,
    spec: NewComponentSpec,
}

impl ComponentSpecEvent {
    #[must_use]
    pub const fn new(
        component_id: ComponentUuid,
        generation: u64,
        time_recorded: SystemTime,
        spec: NewComponentSpec,
    ) -> Self {
        Self {
            component_id,
            generation,
            time_recorded,
            spec,
        }
    }
}

/// Folded history of one component stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentHistory {
    component_id: ComponentUuid,
    versions: Vec<Component>,
}

/// Invalid component stream history.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ComponentHistoryError {
    #[error("component stream contains another component identity")]
    WrongComponent,
    #[error("component generation is not contiguous")]
    NonContiguous,
}

impl ComponentHistory {
    #[must_use]
    pub const fn new(component_id: ComponentUuid) -> Self {
        Self {
            component_id,
            versions: Vec::new(),
        }
    }

    pub fn apply(&mut self, event: ComponentSpecEvent) -> Result<(), ComponentHistoryError> {
        if event.component_id != self.component_id {
            return Err(ComponentHistoryError::WrongComponent);
        }
        let expected = self.versions.len() as u64;
        if event.generation != expected {
            return Err(ComponentHistoryError::NonContiguous);
        }
        let time_created = self
            .versions
            .first()
            .map(Component::time_created)
            .unwrap_or(event.time_recorded);
        self.versions.push(Component::new(
            self.component_id,
            event.generation,
            time_created,
            event.time_recorded,
            event.spec,
        ));
        Ok(())
    }

    #[must_use]
    pub fn latest(&self) -> Option<&Component> {
        self.versions.last()
    }

    #[must_use]
    pub fn at_generation(&self, generation: u64) -> Option<&Component> {
        usize::try_from(generation)
            .ok()
            .and_then(|generation| self.versions.get(generation))
    }

    #[must_use]
    pub fn next_generation(&self) -> u64 {
        self.versions.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::time::UNIX_EPOCH;

    use super::*;

    fn spec(name: &str) -> NewComponentSpec {
        NewComponentSpec::new(name, "test".parse().unwrap(), &[], Vec::new(), &[]).unwrap()
    }

    #[test]
    fn component_metadata_is_derived_from_its_stream() {
        let component_id = ComponentUuid::from_bytes([1; 16]);
        let created = UNIX_EPOCH + Duration::from_millis(10);
        let modified = UNIX_EPOCH + Duration::from_millis(20);
        let mut history = ComponentHistory::new(component_id);

        history
            .apply(ComponentSpecEvent::new(
                component_id,
                0,
                created,
                spec("first"),
            ))
            .unwrap();
        history
            .apply(ComponentSpecEvent::new(
                component_id,
                1,
                modified,
                spec("second"),
            ))
            .unwrap();

        let component = history.at_generation(1).unwrap();
        assert_eq!(component.id(), component_id);
        assert_eq!(component.generation(), 1);
        assert_eq!(component.time_created(), created);
        assert_eq!(component.time_modified(), modified);
        assert_eq!(component.spec().name(), "second");
    }
}
