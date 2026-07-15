use serde::Deserialize;
use serde::Serialize;

/// A native value transported between an evaluator, a plan, and a controller.
///
/// Core stores and compares it but does not interpret its shape.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NativeValue(serde_json::Value);

impl NativeValue {
    #[must_use]
    pub const fn new(value: serde_json::Value) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_json(&self) -> &serde_json::Value {
        &self.0
    }

    #[must_use]
    pub fn into_json(self) -> serde_json::Value {
        self.0
    }
}

impl From<serde_json::Value> for NativeValue {
    fn from(value: serde_json::Value) -> Self {
        Self::new(value)
    }
}
