//! Sorted-string table data file format

use std::path::PathBuf;

use crate::{Error, Result};
use rb_tree::RBMap;
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};

use super::Value;

// TODO this implementation loads the entire sstable into memory to
// search for a value.  A better approach would be serialize the
// sstable into an index block and a data block.  The data block is
// all of the values from the memtable serialized and concatenated.
// The index block is a serialized HashMap<String, u64> that maps keys
// to offsets into the data block.  This approach means we just need
// to load the keys into memory to search the stream instead of the
// values as well.

pub struct SSTable {
    filepath: PathBuf,
}

impl SSTable {
    pub fn new<P: Into<PathBuf>>(filepath: P) -> Self {
        Self {
            filepath: filepath.into(),
        }
    }

    async fn file_handle(&self) -> Result<File> {
        match OpenOptions::new()
            .create(true)
            .write(true)
            .open(&self.filepath)
            .await
        {
            Ok(f) => Ok(f),
            Err(e) => Err(Error::IO(e)),
        }
    }

    /// Writes the memtable to disk as an SSTable
    pub async fn write(&self, memtable: &RBMap<String, Value>) -> Result<()> {
        match fs::metadata(&self.filepath).await {
            Ok(_) => Err(Error::E(format!(
                "File {} already exists",
                &self.filepath.to_str().unwrap()
            ))),
            Err(_) => Ok(()),
        }?;
        let mut file = self.file_handle().await?;
        let buf = &mut bincode::serialize(memtable)?;
        file.write_all(buf.as_slice()).await?;
        Ok(())
    }

    /// Returns the value associated with the key if it exists in the SSTable.
    pub async fn search(&self, key: String) -> Result<Option<Value>> {
        let mut file = self.file_handle().await?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;
        let memtable: RBMap<String, Value> = bincode::deserialize(buf.as_slice())?;
        Ok(memtable.get(&key).map(|v| v.to_owned()))
    }
}
