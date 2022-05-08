//! Sorted-string table data file format
//!
//! # File Format
//! The files consist of:
//!   1. A u64 representing the size of the index block
//!   2. The index block, which is a bincode-serialized BTreeMap<String, IndexEntry>
//!   3. The data block, consisting of concatenated bincode-serialized Value objects
//!
//! To find a value, the index is loaded into memory, then searched to
//! yield the offset and size of the Value associated with the
//! searched-for key.

use std::{collections::BTreeMap, io::SeekFrom, mem, path::PathBuf};

use crate::{Error, Result};
use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWriteExt},
};

use super::Value;

type Index = BTreeMap<String, IndexEntry>;

#[derive(Debug, Serialize, Deserialize)]
struct IndexEntry {
    // Byte offset from the beginning of the SSTable file where the value is stored
    offset: u64,
    // Size in bytes of the value block
    size: u64,
}

#[derive(Debug)]
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
            .read(true)
            .write(true)
            .create(true)
            .open(&self.filepath)
            .await
        {
            Ok(f) => Ok(f),
            Err(e) => Err(Error::IO(e)),
        }
    }

    /// Writes the memtable to disk as an SSTable
    pub async fn write(&self, memtable: &BTreeMap<String, Value>) -> Result<()> {
        match fs::metadata(&self.filepath).await {
            Ok(_) => Err(Error::E(format!(
                "File {} already exists",
                &self.filepath.to_str().unwrap()
            ))),
            Err(_) => Ok(()),
        }?;
        let mut offset = 0;
        let mut index = BTreeMap::new();
        for (key, val) in memtable.iter() {
            let size = bincode::serialized_size(val)?;
            index.insert(key.clone(), IndexEntry { offset, size });
            offset += size + 1;
        }
        let index_size = bincode::serialized_size(&index)?;
        for (_, val) in index.iter_mut() {
            val.offset += mem::size_of::<u64>() as u64 + index_size;
        }
        let buf = bincode::serialize(&index)?;
        let mut file = self.file_handle().await?;
        file.write_u64(index_size).await?;
        file.write_all(buf.as_slice()).await?;
        for val in memtable.values() {
            file.write_all(bincode::serialize(val)?.as_slice()).await?;
        }
        file.sync_all().await?;
        Ok(())
    }

    async fn read_index<R: AsyncRead + Unpin>(&self, reader: &mut R) -> Result<Index> {
        let index_size = reader.read_u64().await?;
        let mut buf = BytesMut::with_capacity(self::u64_to_usize(index_size));
        reader.read_buf(&mut buf).await?;
        let index = bincode::deserialize(buf.as_ref())?;
        Ok(index)
    }

    async fn read_value<R: AsyncRead + AsyncSeek + Unpin>(
        &self,
        reader: &mut R,
        index_entry: &IndexEntry,
    ) -> Result<Value> {
        let IndexEntry { offset, size } = index_entry;
        let mut buf = BytesMut::with_capacity(self::u64_to_usize(*size));
        reader.seek(SeekFrom::Start(*offset)).await?;
        reader.read_buf(&mut buf).await?;
        let val = bincode::deserialize(buf.as_ref())?;
        Ok(val)
    }

    /// Returns the value associated with the key if it exists in the SSTable.
    pub async fn search(&self, key: String) -> Result<Option<Value>> {
        let mut file = self.file_handle().await?;
        let index = self.read_index(&mut file).await?;
        match index.get(&key) {
            Some(index_entry) => self
                .read_value(&mut file, index_entry)
                .await
                .map(Option::Some),
            None => Ok(None),
        }
    }

    pub async fn scan(
        &self,
        from_inclusive: &str,
        to_exclusive: &str,
    ) -> Result<Vec<(String, Value)>> {
        let mut file = self.file_handle().await?;
        let index = self.read_index(&mut file).await?;
        let mut result = Vec::new();
        for (key, index_entry) in index.range(from_inclusive.to_string()..to_exclusive.to_string())
        {
            let val = self.read_value(&mut file, index_entry).await?;
            result.push((key.clone(), val));
        }
        Ok(result)
    }

    pub async fn keys(&self) -> Result<Vec<String>> {
        let mut file = self.file_handle().await?;
        let index = self.read_index(&mut file).await?;
        Ok(index.into_keys().collect())
    }
}

fn u64_to_usize(input: u64) -> usize {
    // Annoyingly, bincode::deserialized_size returns a u64 but
    // BytesMut::with_capacity expects a usize. This is a bad way to
    // solve that problem so hopefully we'll come up with something
    // better at some point
    usize::try_from(input).expect("32 bit architecture not supported")
}

#[cfg(test)]
mod tests {
    use std::{env, path::PathBuf};

    use crate::{store::lsm::Value, Result};
    use maplit::btreemap;
    use uuid::Uuid;

    use super::SSTable;

    fn test_data_file() -> PathBuf {
        env::temp_dir().join(format!("{}.sst", Uuid::new_v4().to_string()))
    }

    #[tokio::test]
    async fn test_read_write() -> Result<()> {
        let path = self::test_data_file();
        let sstable = SSTable::new(path);
        let memtable = btreemap! {
            "bar".to_string() => Value::Data(b"qux".to_vec()),
            "foo".to_string() => Value::Data(b"bar".to_vec()),
            "qux".to_string() => Value::Data(b"boom".to_vec()),
            "zip".to_string() => Value::Tombstone,
        };
        sstable.write(&memtable).await?;
        assert_eq!(
            Some(Value::Data(b"qux".to_vec())),
            sstable.search("bar".to_string()).await?
        );
        assert_eq!(
            Some(Value::Tombstone),
            sstable.search("zip".to_string()).await?
        );
        assert_eq!(None, sstable.search("missing".to_string()).await?);
        assert_eq!(
            vec![
                ("bar".to_string(), Value::Data(b"qux".to_vec())),
                ("foo".to_string(), Value::Data(b"bar".to_vec())),
                ("qux".to_string(), Value::Data(b"boom".to_vec()))
            ],
            sstable.scan("bar", "quxx").await?
        );
        assert_eq!(
            vec![
                "bar".to_string(),
                "foo".to_string(),
                "qux".to_string(),
                "zip".to_string()
            ],
            sstable.keys().await?
        );
        Ok(())
    }
}
