use crate::ComponentSpecHash;
use crate::ConnectorKey;
use crate::Fingerprint;
use crate::PublicationUuid;
use crate::RequestUuid;
use iddqd::IdOrdItem;
use iddqd::id_upcast;

#[derive(Clone, Debug, Eq, PartialEq)]
/// JSON outputs published for one component-spec identity.
pub struct ComponentOutputs {
    component_spec_hash: ComponentSpecHash,
    values_json: Vec<u8>,
}

impl ComponentOutputs {
    #[must_use]
    pub const fn new(component_spec_hash: ComponentSpecHash, values_json: Vec<u8>) -> Self {
        Self {
            component_spec_hash,
            values_json,
        }
    }

    #[must_use]
    pub const fn component_spec_hash(&self) -> ComponentSpecHash {
        self.component_spec_hash
    }

    #[must_use]
    pub fn values_json(&self) -> &[u8] {
        &self.values_json
    }
}

impl IdOrdItem for ComponentOutputs {
    type Key<'a> = ComponentSpecHash;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.component_spec_hash
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Latest durable outputs published by one connector.
pub struct PublishedSliceOutputs {
    generation: u64,
    connector: ConnectorKey,
    outputs: Vec<ComponentOutputs>,
    publication_sequence: u64,
    publication_id: PublicationUuid,
    input_sequence: u64,
}

impl PublishedSliceOutputs {
    #[doc(hidden)]
    #[must_use]
    pub const fn new(
        generation: u64,
        connector: ConnectorKey,
        outputs: Vec<ComponentOutputs>,
        publication_sequence: u64,
        publication_id: PublicationUuid,
        input_sequence: u64,
    ) -> Self {
        Self {
            generation,
            connector,
            outputs,
            publication_sequence,
            publication_id,
            input_sequence,
        }
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn connector(&self) -> &ConnectorKey {
        &self.connector
    }

    #[must_use]
    pub fn outputs(&self) -> &[ComponentOutputs] {
        &self.outputs
    }

    #[must_use]
    pub const fn publication_sequence(&self) -> u64 {
        self.publication_sequence
    }

    #[must_use]
    pub const fn publication_id(&self) -> PublicationUuid {
        self.publication_id
    }

    #[must_use]
    pub const fn input_sequence(&self) -> u64 {
        self.input_sequence
    }
}

impl IdOrdItem for PublishedSliceOutputs {
    type Key<'a> = &'a ConnectorKey;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        &self.connector
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Durable publication event before its graph-stream sequence is assigned.
pub struct OutputPublication {
    pub generation: u64,
    pub input_sequence: u64,
    pub connector: ConnectorKey,
    pub outputs: Vec<ComponentOutputs>,
    pub request_id: RequestUuid,
    pub request_fingerprint: Fingerprint,
    pub publication_id: PublicationUuid,
    pub publication_fingerprint: Fingerprint,
}
