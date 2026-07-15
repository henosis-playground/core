use rand::RngCore;
use rand::SeedableRng;
use rand_chacha::ChaCha12Rng;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Seed([u8; 32]);

impl Seed {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub fn from_u64(value: u64) -> Self {
        let mut bytes = [0; 32];
        bytes[..8].copy_from_slice(&value.to_le_bytes());
        Self(*blake3::hash(&bytes).as_bytes())
    }

    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }

    #[must_use]
    pub fn child(self, name: &str) -> Self {
        let mut hasher = blake3::Hasher::new_keyed(&self.0);
        hasher.update(name.as_bytes());
        Self(*hasher.finalize().as_bytes())
    }
}

#[derive(Clone, Debug)]
pub struct NamedRng {
    root: Seed,
    name: String,
    rng: ChaCha12Rng,
}

impl NamedRng {
    #[must_use]
    pub fn new(root: Seed, name: impl Into<String>) -> Self {
        let name = name.into();
        let child = root.child(&name);
        Self {
            root,
            name,
            rng: ChaCha12Rng::from_seed(child.as_bytes()),
        }
    }

    #[must_use]
    pub fn child(&self, name: &str) -> Self {
        Self::new(self.root, format!("{}/{}", self.name, name))
    }

    #[must_use]
    pub const fn root_seed(&self) -> Seed {
        self.root
    }

    #[must_use]
    pub fn stream_seed(&self) -> Seed {
        self.root.child(&self.name)
    }

    #[must_use]
    pub fn choose_index(&mut self, length: usize) -> Option<usize> {
        if length == 0 {
            return None;
        }
        Some((self.rng.next_u64() % length as u64) as usize)
    }

    #[must_use]
    pub fn next_u64(&mut self) -> u64 {
        self.rng.next_u64()
    }
}
