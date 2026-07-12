use diesel::result::DatabaseErrorKind;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::bb8::Pool;
use faultline::Error as Fault;
use scoped_futures::ScopedBoxFuture;

/// Internal bridge between `Diesel`'s transaction error bound and faultline.
enum TransactionError<Domain> {
    Diesel(diesel::result::Error),
    Classified(Fault<Domain, anyhow::Error, anyhow::Error>),
}

impl<Domain> From<diesel::result::Error> for TransactionError<Domain> {
    fn from(error: diesel::result::Error) -> Self {
        Self::Diesel(error)
    }
}

impl<Domain> From<Fault<Domain, anyhow::Error, anyhow::Error>> for TransactionError<Domain> {
    fn from(error: Fault<Domain, anyhow::Error, anyhow::Error>) -> Self {
        Self::Classified(error)
    }
}

impl<Domain> TransactionError<Domain> {
    fn into_fault(self) -> Fault<Domain, anyhow::Error, anyhow::Error> {
        match self {
            Self::Classified(error) => error,
            Self::Diesel(error) => match &error {
                diesel::result::Error::DatabaseError(
                    DatabaseErrorKind::SerializationFailure | DatabaseErrorKind::ClosedConnection,
                    _,
                ) => Fault::Transient(anyhow::Error::new(error)),
                _ => Fault::Invariant(anyhow::Error::new(error)),
            },
        }
    }
}

/// Database pool that enforces transactional access to metadata datastores.
///
/// Datastore methods operate on [`AsyncPgConnection`] and assume the caller
/// supplied a transaction through [`transaction`](Self::transaction).
#[derive(Clone, Debug)]
pub struct DbPool {
    primary: Pool<AsyncPgConnection>,
}

impl DbPool {
    /// Wrap an already-configured asynchronous `PostgreSQL` pool.
    #[must_use]
    pub const fn new(primary: Pool<AsyncPgConnection>) -> Self {
        Self { primary }
    }

    /// Build the service's default asynchronous `PostgreSQL` pool.
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
        let primary = Pool::builder().max_size(8).build(manager).await?;
        Ok(Self::new(primary))
    }

    /// Execute a datastore operation in a serializable transaction.
    pub async fn transaction<'a, Value, Domain, Operation>(
        &self,
        operation: Operation,
    ) -> Result<Value, Fault<Domain, anyhow::Error, anyhow::Error>>
    where
        Operation: for<'connection> FnOnce(
                &'connection mut AsyncPgConnection,
            ) -> ScopedBoxFuture<
                'a,
                'connection,
                Result<Value, Fault<Domain, anyhow::Error, anyhow::Error>>,
            > + Send
            + 'a,
        Value: Send + 'a,
        Domain: Send + 'a,
    {
        let mut connection = self.primary.get().await.map_err(|error| {
            Fault::<Domain, anyhow::Error, anyhow::Error>::Transient(anyhow::Error::new(error))
        })?;

        connection
            .build_transaction()
            .serializable()
            .run(async |connection| operation(connection).await.map_err(TransactionError::from))
            .await
            .map_err(TransactionError::into_fault)
    }
}
