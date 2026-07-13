//! Protobuf/domain boundary conversions.

macro_rules! wire_field {
    ($message:ident. $field:ident) => {
        crate::convert::field::Field::new(stringify!($field), &$message.$field)
    };
}

pub(crate) use wire_field;

pub(crate) mod field;
mod fingerprint;
mod request;
mod value;

use std::fmt;

pub use fingerprint::*;
pub use value::reconcile_slice_request;
pub use value::register_component_spec;
pub use value::retire_slice_request;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConversionError {
    path: Vec<FieldPathElement>,
    kind: ConversionErrorKind,
}

impl ConversionError {
    fn missing(element: FieldPathElement) -> Self {
        Self {
            path: vec![element],
            kind: ConversionErrorKind::Missing,
        }
    }

    fn invalid(element: FieldPathElement, error: impl fmt::Display) -> Self {
        Self {
            path: vec![element],
            kind: ConversionErrorKind::Invalid(error.to_string()),
        }
    }

    fn prepend(mut self, element: FieldPathElement) -> Self {
        self.path.insert(0, element);
        self
    }
}

impl fmt::Display for ConversionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ConversionErrorKind::Missing => write!(formatter, "missing required field ")?,
            ConversionErrorKind::Invalid(_) => write!(formatter, "invalid field ")?,
        }
        for (position, element) in self.path.iter().enumerate() {
            if position > 0 {
                formatter.write_str(".")?;
            }
            write!(formatter, "{element}")?;
        }
        if let ConversionErrorKind::Invalid(message) = &self.kind {
            write!(formatter, ": {message}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ConversionError {}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ConversionErrorKind {
    Missing,
    Invalid(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FieldPathElement {
    field: &'static str,
    index: Option<usize>,
}

impl FieldPathElement {
    const fn new(field: &'static str) -> Self {
        Self { field, index: None }
    }

    const fn with_index(&self, index: usize) -> Self {
        Self {
            field: self.field,
            index: Some(index),
        }
    }
}

impl fmt::Display for FieldPathElement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.field)?;
        if let Some(index) = self.index {
            write!(formatter, "[{index}]")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use buffa::Message;
    use buffa::MessageField;
    use buffa::MessageView;
    use henosis_types as domain;

    use crate::proto::henosis::v1 as pb;
    use crate::proto::henosis::v1::__buffa::view;

    #[test]
    fn nested_conversion_prepends_its_field() {
        let request = pb::RegisterComponentSpecRequest {
            spec: MessageField::some(pb::ComponentSpec {
                connector: Some("k8s".to_owned()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let bytes = request.encode_to_vec();
        let request = view::RegisterComponentSpecRequestView::decode_view(&bytes).unwrap();

        let error = domain::RegisterComponentSpec::try_from(&request).unwrap_err();

        assert_eq!(error.to_string(), "missing required field spec.name");
    }

    #[test]
    fn repeated_conversion_includes_the_item_index() {
        let request = pb::UpdateComponentsRequest {
            graph_id: Some([1; 16].to_vec()),
            expected_generation: Some(1),
            replacements: vec![pb::ComponentReplacement {
                current_spec_hash: Some([2; 32].to_vec()),
                replacement_spec_hash: Some(vec![3]),
                ..Default::default()
            }],
            request_id: Some([4; 16].to_vec()),
            ..Default::default()
        };
        let bytes = request.encode_to_vec();
        let request = view::UpdateComponentsRequestView::decode_view(&bytes).unwrap();

        let error = domain::UpdateComponents::try_from(&request).unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid field replacements[0].replacement_spec_hash: must contain exactly 32 bytes, \
             received 1"
        );
    }
}
