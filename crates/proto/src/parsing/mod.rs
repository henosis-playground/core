macro_rules! field {
    ($message:ident. $field:ident) => {
        crate::parsing::extraction::Field::new(stringify!($field), &$message.$field)
    };
}

pub(crate) use field;

mod error;
pub(crate) mod extraction;

pub use error::ConversionError;
