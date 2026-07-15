use std::fmt;
use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

macro_rules! domain_name {
    ($name:ident, $description:literal) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, NameError> {
                let value = value.into();
                validate_name(&value)?;
                Ok(Self(value))
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl FromStr for $name {
            type Err = NameError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = NameError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

domain_name!(GraphName, "graph name");
domain_name!(ComponentName, "component name");
domain_name!(ControllerName, "controller name");
domain_name!(ResourceName, "resource name");
domain_name!(KindName, "resource kind");
domain_name!(OutputName, "output name");

fn validate_name(value: &str) -> Result<(), NameError> {
    if value.is_empty() {
        return Err(NameError::Empty);
    }
    if value.len() > 128 {
        return Err(NameError::TooLong);
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/'))
    {
        return Err(NameError::InvalidCharacter);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum NameError {
    #[error("name must not be empty")]
    Empty,
    #[error("name must be at most 128 bytes")]
    TooLong,
    #[error("name contains a character outside [A-Za-z0-9._/-]")]
    InvalidCharacter,
}
