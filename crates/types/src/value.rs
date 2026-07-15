use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use thiserror::Error;

/// Opaque finite JSON with a canonical serialization computed at construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeValue {
    value: serde_json::Value,
    canonical: String,
}

impl NativeValue {
    pub fn new(value: serde_json::Value) -> Result<Self, NativeValueError> {
        let mut canonical = String::new();
        write_canonical(&value, &mut canonical)?;
        Ok(Self { value, canonical })
    }

    pub fn from_canonical(
        value: serde_json::Value,
        claimed_canonical: &str,
    ) -> Result<Self, NativeValueError> {
        let native = Self::new(value)?;
        if native.canonical != claimed_canonical {
            return Err(NativeValueError::CanonicalMismatch);
        }
        Ok(native)
    }

    #[must_use]
    pub const fn as_json(&self) -> &serde_json::Value {
        &self.value
    }

    #[must_use]
    pub fn canonical(&self) -> &str {
        &self.canonical
    }

    #[must_use]
    pub fn into_json(self) -> serde_json::Value {
        self.value
    }
}

impl TryFrom<serde_json::Value> for NativeValue {
    type Error = NativeValueError;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl Serialize for NativeValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.value.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NativeValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

fn write_canonical(value: &serde_json::Value, target: &mut String) -> Result<(), NativeValueError> {
    match value {
        serde_json::Value::Null => target.push_str("null"),
        serde_json::Value::Bool(value) => target.push_str(if *value { "true" } else { "false" }),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                target.push_str(&value.to_string());
            } else if let Some(value) = value.as_u64() {
                target.push_str(&value.to_string());
            } else {
                let value = value.as_f64().ok_or(NativeValueError::Serialization)?;
                target.push_str(ryu_js::Buffer::new().format_finite(value));
            }
        }
        serde_json::Value::String(value) => target
            .push_str(&serde_json::to_string(value).map_err(|_| NativeValueError::Serialization)?),
        serde_json::Value::Array(values) => {
            target.push('[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    target.push(',');
                }
                write_canonical(value, target)?;
            }
            target.push(']');
        }
        serde_json::Value::Object(values) => {
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| utf16_cmp(left, right));
            target.push('{');
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index != 0 {
                    target.push(',');
                }
                target.push_str(
                    &serde_json::to_string(key).map_err(|_| NativeValueError::Serialization)?,
                );
                target.push(':');
                write_canonical(value, target)?;
            }
            target.push('}');
        }
    }
    Ok(())
}

fn utf16_cmp(left: &str, right: &str) -> std::cmp::Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum NativeValueError {
    #[error("canonical JSON serialization failed")]
    Serialization,
    #[error("claimed canonical JSON does not match the body")]
    CanonicalMismatch,
}

impl std::fmt::Display for NativeValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.canonical)
    }
}

/// JSON-compatible value contract carried by component metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum ValueSchema {
    String,
    Url,
    Number,
    Boolean,
    Json,
    Array {
        element: Box<Self>,
    },
    Object {
        fields: std::collections::BTreeMap<String, Self>,
    },
}

impl ValueSchema {
    #[must_use]
    pub fn accepts(&self, value: &serde_json::Value) -> bool {
        match self {
            Self::String => value.is_string(),
            Self::Url => value
                .as_str()
                .is_some_and(|value| value.starts_with("http://") || value.starts_with("https://")),
            Self::Number => value.is_number(),
            Self::Boolean => value.is_boolean(),
            Self::Json => true,
            Self::Array { element } => value
                .as_array()
                .is_some_and(|values| values.iter().all(|value| element.accepts(value))),
            Self::Object { fields } => value.as_object().is_some_and(|value| {
                fields.iter().all(|(name, schema)| {
                    value.get(name).is_some_and(|value| schema.accepts(value))
                })
            }),
        }
    }
}

impl std::fmt::Display for ValueSchema {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String => formatter.write_str("string"),
            Self::Url => formatter.write_str("absolute HTTP(S) URL"),
            Self::Number => formatter.write_str("number"),
            Self::Boolean => formatter.write_str("boolean"),
            Self::Json => formatter.write_str("JSON value"),
            Self::Array { element } => write!(formatter, "array of {element}"),
            Self::Object { fields } => {
                formatter.write_str("object { ")?;
                for (index, (name, schema)) in fields.iter().enumerate() {
                    if index != 0 {
                        formatter.write_str(", ")?;
                    }
                    write!(formatter, "{name}: {schema}")?;
                }
                formatter.write_str(" }")
            }
        }
    }
}
