use std::fmt;

use serde::Deserialize;
use serde::Serialize;

/// The content-addressed identity of a component spec.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ComponentSpecHash([u8; 32]);

impl ComponentSpecHash {
    pub const LENGTH: usize = 32;

    #[must_use]
    pub const fn from_bytes(value: [u8; Self::LENGTH]) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; Self::LENGTH] {
        &self.0
    }

    #[must_use]
    pub const fn into_bytes(self) -> [u8; Self::LENGTH] {
        self.0
    }
}

impl fmt::Display for ComponentSpecHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}
