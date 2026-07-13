use std::fmt;
use std::str::FromStr;

use blake3::Hash;
use buffa::MessageFieldView;
use buffa::RepeatedView;
use newtype_uuid::TypedUuid;
use newtype_uuid::TypedUuidKind;
use types::domain::ComponentUuid;

use super::error::ConversionError;
use super::error::FieldPathElement;

pub(crate) struct Field<'a, T> {
    element: FieldPathElement,
    value: &'a T,
}

impl<'a, T> Field<'a, T> {
    pub(crate) const fn new(name: &'static str, value: &'a T) -> Self {
        Self {
            element: FieldPathElement::new(name),
            value,
        }
    }

    pub(crate) fn invalid(&self, error: impl fmt::Display) -> ConversionError {
        ConversionError::invalid(self.element.clone(), error)
    }
}

impl<T: Copy> Field<'_, Option<T>> {
    pub(crate) fn required(&self) -> Result<FieldValue<T>, ConversionError> {
        self.value
            .map(|value| FieldValue::new(self.element.clone(), value))
            .ok_or_else(|| ConversionError::missing(self.element.clone()))
    }

    pub(crate) fn optional(&self) -> Option<FieldValue<T>> {
        self.value
            .map(|value| FieldValue::new(self.element.clone(), value))
    }

    pub(crate) fn or_default(&self) -> FieldValue<T>
    where
        T: Default,
    {
        FieldValue::new(self.element.clone(), self.value.unwrap_or_default())
    }
}

impl<'a, V> Field<'a, MessageFieldView<V>> {
    pub(crate) fn required(&self) -> Result<FieldValue<&'a V>, ConversionError> {
        self.value
            .as_option()
            .map(|value| FieldValue::new(self.element.clone(), value))
            .ok_or_else(|| ConversionError::missing(self.element.clone()))
    }

    pub(crate) fn optional(&self) -> Option<FieldValue<&'a V>> {
        self.value
            .as_option()
            .map(|value| FieldValue::new(self.element.clone(), value))
    }
}

impl<'a, T> Field<'a, RepeatedView<'a, T>> {
    pub(crate) fn iter(&self) -> impl Iterator<Item = FieldValue<&T>> {
        self.value
            .iter()
            .enumerate()
            .map(|(index, value)| FieldValue::new(self.element.with_index(index), value))
    }
}

pub(crate) struct FieldValue<T> {
    element: FieldPathElement,
    value: T,
}

impl<T> FieldValue<T> {
    const fn new(element: FieldPathElement, value: T) -> Self {
        Self { element, value }
    }

    pub(crate) fn into_inner(self) -> T {
        self.value
    }

    pub(crate) const fn value(&self) -> &T {
        &self.value
    }

    pub(crate) fn validate(
        self,
        predicate: impl FnOnce(&T) -> bool,
        message: &'static str,
    ) -> Result<T, ConversionError> {
        if predicate(&self.value) {
            Ok(self.value)
        } else {
            Err(ConversionError::invalid(self.element, message))
        }
    }

    pub(crate) fn convert<U>(self) -> Result<U, ConversionError>
    where
        U: TryFrom<T, Error = ConversionError>,
    {
        U::try_from(self.value).map_err(|error| error.prepend(self.element))
    }

    pub(crate) fn parse<U>(self) -> Result<U, ConversionError>
    where
        T: AsRef<str>,
        U: FromStr,
        U::Err: fmt::Display,
    {
        U::from_str(self.value.as_ref())
            .map_err(|error| ConversionError::invalid(self.element, error))
    }

    pub(crate) fn uuid<K>(self) -> Result<TypedUuid<K>, ConversionError>
    where
        T: AsRef<[u8]>,
        K: TypedUuidKind,
    {
        self.fixed_bytes().map(TypedUuid::from_bytes)
    }

    pub(crate) fn component_id(self) -> Result<ComponentUuid, ConversionError>
    where
        T: AsRef<[u8]>,
    {
        self.fixed_bytes().map(ComponentUuid::from_bytes)
    }

    pub(crate) fn hash(self) -> Result<Hash, ConversionError>
    where
        T: AsRef<[u8]>,
    {
        self.fixed_bytes().map(Hash::from_bytes)
    }

    pub(crate) fn fixed_bytes<const LENGTH: usize>(self) -> Result<[u8; LENGTH], ConversionError>
    where
        T: AsRef<[u8]>,
    {
        let value = self.value.as_ref();
        value.try_into().map_err(|_| {
            ConversionError::invalid(
                self.element,
                format_args!(
                    "must contain exactly {LENGTH} bytes, received {}",
                    value.len()
                ),
            )
        })
    }
}

impl<T> FieldValue<T>
where
    T: AsRef<[u8]>,
{
    pub(crate) fn json(self) -> Result<Vec<u8>, ConversionError> {
        let value = serde_json::from_slice::<serde_json::Value>(self.value.as_ref())
            .map_err(|error| ConversionError::invalid(self.element.clone(), error))?;
        serde_json::to_vec(&value).map_err(|error| ConversionError::invalid(self.element, error))
    }
}
