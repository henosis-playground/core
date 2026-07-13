/// A canonical semantic request or publication digest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Fingerprint([u8; 32]);

impl Fingerprint {
    pub const LENGTH: usize = 32;

    #[must_use]
    pub const fn from_bytes(value: [u8; Self::LENGTH]) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; Self::LENGTH] {
        &self.0
    }
}
