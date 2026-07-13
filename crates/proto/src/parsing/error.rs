use std::fmt;

use types::domain;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConversionError {
    path: Vec<FieldPathElement>,
    kind: ConversionErrorKind,
}

impl ConversionError {
    pub(super) fn missing(element: FieldPathElement) -> Self {
        Self {
            path: vec![element],
            kind: ConversionErrorKind::Missing,
        }
    }

    pub(super) fn invalid(element: FieldPathElement, error: impl fmt::Display) -> Self {
        Self {
            path: vec![element],
            kind: ConversionErrorKind::Invalid(error.to_string()),
        }
    }

    pub(super) fn prepend(mut self, element: FieldPathElement) -> Self {
        self.path.insert(0, element);
        self
    }
}

impl fmt::Display for ConversionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ConversionErrorKind::Missing => formatter.write_str("missing required field ")?,
            ConversionErrorKind::Invalid(_) => formatter.write_str("invalid field ")?,
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

impl From<domain::ComponentSpecError> for ConversionError {
    fn from(error: domain::ComponentSpecError) -> Self {
        let field = match error {
            domain::ComponentSpecError::EmptyName => "name",
            domain::ComponentSpecError::DuplicateDependency => "depends_on",
        };
        Self::invalid(FieldPathElement::new(field), error)
    }
}

impl From<domain::ContractFailureDetailError> for ConversionError {
    fn from(error: domain::ContractFailureDetailError) -> Self {
        match error {
            domain::ContractFailureDetailError::DuplicateConsumedPath => {
                Self::invalid(FieldPathElement::new("consumed_paths"), error)
            }
        }
    }
}

impl From<domain::GraphError> for ConversionError {
    fn from(error: domain::GraphError) -> Self {
        let field = match error {
            domain::GraphError::InvalidGeneration => "generation",
            domain::GraphError::DuplicateComponent => "component_ids",
        };
        Self::invalid(FieldPathElement::new(field), error)
    }
}

impl From<domain::SliceReportError> for ConversionError {
    fn from(error: domain::SliceReportError) -> Self {
        let field = match error {
            domain::SliceReportError::InvalidGeneration => "generation",
            domain::SliceReportError::DuplicateDisposition => "dispositions",
            domain::SliceReportError::DuplicateOutput => "outputs",
        };
        Self::invalid(FieldPathElement::new(field), error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ConversionErrorKind {
    Missing,
    Invalid(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FieldPathElement {
    field: &'static str,
    index: Option<usize>,
}

impl FieldPathElement {
    pub(super) const fn new(field: &'static str) -> Self {
        Self { field, index: None }
    }

    pub(super) const fn with_index(&self, index: usize) -> Self {
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
