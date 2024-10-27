use std::path::PathBuf;

pub struct Context {
    pub storage: PathBuf,
    pub cluster_data: PathBuf,
}

impl Context {
    pub fn new(storage: PathBuf, cluster_data: PathBuf) -> Self {
        Self {
            storage,
            cluster_data,
        }
    }
}
