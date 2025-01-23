use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub id: Uuid,
    pub created_at: i64,
    pub label: String,
    pub checksum: blake3::Hash,
}
