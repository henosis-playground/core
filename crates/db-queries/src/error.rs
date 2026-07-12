use diesel::result::DatabaseErrorKind;
use faultline::Error as Fault;
use faultline::Never;

pub(crate) fn diesel_fault(
    error: diesel::result::Error,
) -> Fault<Never, anyhow::Error, anyhow::Error> {
    match &error {
        diesel::result::Error::DatabaseError(
            DatabaseErrorKind::SerializationFailure | DatabaseErrorKind::ClosedConnection,
            _,
        ) => Fault::Transient(anyhow::Error::new(error)),
        _ => Fault::Invariant(anyhow::Error::new(error)),
    }
}

pub(crate) fn invariant(
    error: impl std::error::Error + Send + Sync + 'static,
) -> Fault<Never, anyhow::Error, anyhow::Error> {
    Fault::Invariant(anyhow::Error::new(error))
}
