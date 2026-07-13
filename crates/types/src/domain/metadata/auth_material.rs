/// Enabled administrative authentication material loaded from the datastore.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthMaterial {
    key: String,
    token_hash: Vec<u8>,
    enabled: bool,
}

impl AuthMaterial {
    /// Construct a value loaded and validated by the datastore boundary.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(key: String, token_hash: Vec<u8>, enabled: bool) -> Self {
        Self {
            key,
            token_hash,
            enabled,
        }
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    #[must_use]
    pub fn token_hash(&self) -> &[u8] {
        &self.token_hash
    }

    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }
}
