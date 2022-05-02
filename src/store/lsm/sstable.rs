//! Sorted-string table data file format

use std::path::{Path, PathBuf};

use crate::{Error, Result};
use rb_tree::RBMap;
use tokio::fs::{self, File, OpenOptions};

pub struct SSTable {
    index_path: PathBuf,
    data_path: PathBuf,
}

impl SSTable {
    pub fn new<P: Into<PathBuf>>(index_path: P, data_path: P) -> Self {
        Self {
            index_path: index_path.into(),
            data_path: data_path.into(),
        }
    }

    async fn file_handle(path: &Path) -> Result<File> {
        match OpenOptions::new().create(true).write(true).open(path).await {
            Ok(f) => Ok(f),
            Err(e) => Err(Error::IO(e)),
        }
    }

    /// Writes the memtable to disk as an SSTable
    pub async fn write(&self, memtable: &RBMap<String, Option<Vec<u8>>>) -> Result<()> {
        match fs::metadata(&self.index_path).await {
            Ok(_) => Err(Error::E(format!(
                "File {} already exists",
                &self.index_path.to_str().unwrap()
            ))),
            Err(_) => Ok(()),
        }?;
        match fs::metadata(&self.index_path).await {
            Ok(_) => Err(Error::E(format!(
                "File {} already exists",
                &self.index_path.to_str().unwrap()
            ))),
            Err(_) => Ok(()),
        }?;
        let index_file = Self::file_handle(self.index_path.as_path()).await?;
        let data_file = Self::file_handle(self.data_path.as_path()).await?;
        for (key, val) in memtable.iter() {
            let key = key.clone();
            let val = val.clone();
            // Calculate size of KV pair and offset into the data file, write that to the index file
        }
        Ok(())
    }

    /// Returns the value associated with the key if it exists in the SSTable.
    pub async fn search(&self, key: String) -> Result<Option<Vec<u8>>> {}
}
