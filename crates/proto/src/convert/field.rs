use std::fmt;
use std::str::FromStr;

use buffa::MessageFieldView;
use buffa::RepeatedView;
use henosis_types::ComponentSpecHash;
use henosis_types::Fingerprint;
use newtype_uuid::TypedUuid;
use newtype_uuid::TypedUuidKind;

use super::ConversionError;
use super::FieldPathElement;

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
    pub(crate) fn required(&self) -> Result<Located<T>, ConversionError> {
        self.value
            .map(|value| Located::new(self.element.clone(), value))
            .ok_or_else(|| ConversionError::missing(self.element.clone()))
    }

    pub(crate) fn optional(&self) -> Option<Located<T>> {
        self.value
            .map(|value| Located::new(self.element.clone(), value))
    }

    pub(crate) fn or_default(&self) -> Located<T>
    where
        T: Default,
    {
        Located::new(self.element.clone(), self.value.unwrap_or_default())
    }
}

impl<'a, V> Field<'a, MessageFieldView<V>> {
    pub(crate) fn required(&self) -> Result<Located<&'a V>, ConversionError> {
        self.value
            .as_option()
            .map(|value| Located::new(self.element.clone(), value))
            .ok_or_else(|| ConversionError::missing(self.element.clone()))
    }

    pub(crate) fn optional(&self) -> Option<Located<&'a V>> {
        self.value
            .as_option()
            .map(|value| Located::new(self.element.clone(), value))
    }
}

impl<'a, T> Field<'a, RepeatedView<'a, T>> {
    pub(crate) fn iter(&self) -> impl Iterator<Item = Located<&T>> {
        self.value
            .iter()
            .enumerate()
            .map(|(index, value)| Located::new(self.element.with_index(index), value))
    }
}

pub(crate) struct Located<T> {
    element: FieldPathElement,
    value: T,
}

impl<T> Located<T> {
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

    pub(crate) fn spec_hash(self) -> Result<ComponentSpecHash, ConversionError>
    where
        T: AsRef<[u8]>,
    {
        self.fixed_bytes().map(ComponentSpecHash::from_bytes)
    }

    pub(crate) fn fingerprint(self) -> Result<Fingerprint, ConversionError>
    where
        T: AsRef<[u8]>,
    {
        self.fixed_bytes().map(Fingerprint::from_bytes)
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

impl<T: ?Sized> Located<&T>
where
    T: ToOwned,
{
    pub(crate) fn owned(self) -> T::Owned {
        self.value.to_owned()
    }
}

impl<T> Located<T>
where
    T: AsRef<[u8]>,
{
    pub(crate) fn json(self) -> Result<Vec<u8>, ConversionError> {
        let value = serde_json::from_slice::<serde_json::Value>(self.value.as_ref())
            .map_err(|error| ConversionError::invalid(self.element.clone(), error))?;
        serde_json::to_vec(&value).map_err(|error| ConversionError::invalid(self.element, error))
    }
}
