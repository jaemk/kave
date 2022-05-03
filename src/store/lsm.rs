mod commit_log;
mod sstable;

use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bloom_filter_rs::{BloomFilter, Murmur3};
use rb_tree::RBMap;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::sync::RwLock;
use uuid::Uuid;

use self::commit_log::CommitLog;
use self::sstable::SSTable;
use self::Value::{Data, Tombstone};

use super::Operation::{Delete, Set};
use super::{Store, Transaction};
use crate::Result;
use crate::{utils, Config};

type Shared<T> = Arc<RwLock<T>>;

/// A store backed by a [log-structured merge tree](http://www.benstopford.com/2015/02/14/log-structured-merge-trees)
#[derive(Clone)]
pub struct LSMStore {
    data: Shared<LSMData>,
    commit_log: Shared<CommitLog>,
    bloom_filter: Shared<BloomFilter<Murmur3>>,
    data_dir: PathBuf,
    memtable_max_bytes: usize,
}

struct LSMData {
    memtable: RBMap<String, Value>,
    tx_ids: Vec<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Value {
    Data(Vec<u8>),
    Tombstone,
}

impl Value {
    pub fn as_option(&self) -> Option<Vec<u8>> {
        match self {
            Data(data) => Some(data.to_vec()),
            Tombstone => None,
        }
    }
}

impl LSMStore {
    pub fn new(data_dir: &Path, commit_log_path: &Path, memtable_max_bytes: usize) -> Self {
        let commit_log = CommitLog::new(commit_log_path);
        Self {
            data: Arc::new(RwLock::new(LSMData {
                memtable: RBMap::new(),
                tx_ids: Vec::new(),
            })),
            commit_log: Arc::new(RwLock::new(commit_log)),
            // TODO what is the optimal number of items for the bloom filter?
            bloom_filter: Arc::new(RwLock::new(BloomFilter::optimal(Murmur3, 512, 0.01))),
            data_dir: data_dir.to_path_buf(),
            memtable_max_bytes,
        }
    }

    pub fn from_config(config: &Config) -> Self {
        Self::new(
            config.data_dir.as_path(),
            config.commit_log_path.as_path(),
            config.memtable_max_mb * 1_000_000,
        )
    }

    pub async fn initialize_from_config(config: &Config) -> Result<Self> {
        let mut store = Self::from_config(config);
        store.initialize().await?;
        Ok(store)
    }

    pub async fn initialize(&mut self) -> Result<()> {
        // TODO once bloom filter is persisted to disk, restore it here
        self.restore_previous_txs().await?;
        self.start_background_tasks();
        Ok(())
    }

    async fn restore_previous_txs(&mut self) -> Result<()> {
        let commit_log_ref = self.commit_log.clone();
        let commit_log = commit_log_ref.read().await;
        for tx in commit_log.get_unfinished_transactions().await? {
            self.transact(tx).await?
        }
        Ok(())
    }

    pub fn start_background_tasks(&self) {
        let data = self.data.clone();
        let data_dir = self.data_dir.clone();
        let commit_log = self.commit_log.clone();
        let memtable_max_bytes = self.memtable_max_bytes;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                if Self::should_flush_memtable(data.clone(), memtable_max_bytes)
                    .await
                    .expect("Failed to size memtable")
                {
                    Self::write_sstable(
                        data.clone(),
                        data_dir.clone().as_path(),
                        commit_log.clone(),
                    )
                    .await
                    .expect("Failed to flush memtable");
                };
            }
        });
        // TODO implement segment compaction
        // TODO persist bloom filter to disk
    }

    /// Whether the memtable has grown big enough to flush to disk.
    async fn should_flush_memtable(
        shared_data: Shared<LSMData>,
        memtable_max_bytes: usize,
    ) -> Result<bool> {
        let data = shared_data.read().await;
        return Ok(
            data.memtable.len() > 0 && mem::size_of_val(&data.memtable) >= memtable_max_bytes
        );
    }

    /// Returns a vector of SSTable paths, ordered from newest to oldest.
    async fn get_sstables(&self) -> Result<Vec<PathBuf>> {
        let mut sstables = Vec::new();
        let mut dir = fs::read_dir(&self.data_dir).await?;
        while let Some(file) = dir.next_entry().await? {
            if let Some(ext) = file.path().extension() {
                if ext == "sst" {
                    sstables.push(file.path());
                };
            };
        }
        sstables.sort_by(|a, b| b.cmp(a));
        Ok(sstables)
    }

    async fn search_sstables(&self, key: &str) -> Result<Option<Value>> {
        for path in self.get_sstables().await? {
            let sstable = SSTable::new(&path);
            let v = sstable.search(key.to_owned()).await?;
            if v.is_some() {
                return Ok(v);
            };
        }
        Ok(None)
    }

    /// Writes the current memtable to disk as an SStable then clears
    /// the memtable.
    async fn write_sstable(
        shared_data: Shared<LSMData>,
        data_dir: &Path,
        commit_log: Shared<CommitLog>,
    ) -> Result<()> {
        let path = data_dir.join(format!("{}.sst", utils::time_since_epoch().as_millis()));
        let sstable = SSTable::new(path);
        let mut data = shared_data.write().await;
        sstable.write(&data.memtable).await?;
        data.memtable = RBMap::new();
        let mut commit_log = commit_log.write().await;
        for tx_id in &data.tx_ids {
            commit_log.end_transaction(tx_id).await?;
        }
        data.tx_ids = Vec::new();
        Ok(())
    }
}

#[async_trait]
impl Store for LSMStore {
    async fn get(&mut self, k: &str) -> Result<Option<Vec<u8>>> {
        let store = self.data.read().await;
        let bloom_filter = self.bloom_filter.read().await;
        if !bloom_filter.contains(k.as_bytes()) {
            return Ok(None);
        }
        let mut result = store
            .memtable
            .get(&k.to_string())
            .and_then(|v| v.as_option());
        if result.is_none() {
            result = self.search_sstables(k).await?.and_then(|v| v.as_option());
        }
        Ok(result)
    }

    async fn transact(&mut self, transaction: Transaction) -> Result<()> {
        let mut commit_log = self.commit_log.write().await;
        let mut data = self.data.write().await;
        let tx_ids = &mut data.tx_ids;
        tx_ids.push(transaction.id.clone());
        commit_log.begin_transaction(&transaction).await?;
        let mut bloom_filter = self.bloom_filter.write().await;
        for instruction in transaction.operations {
            match instruction {
                Set(key, value) => {
                    data.memtable
                        .insert(key.to_string(), Value::Data(value.to_vec()));
                    bloom_filter.insert(key.as_bytes());
                }
                Delete(key) => {
                    data.memtable.insert(key.to_string(), Value::Tombstone);
                }
            };
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{env, time::Duration};

    use tokio::fs::DirBuilder;
    use uuid::Uuid;

    use crate::{
        store::{Operation, Store, Transaction},
        Result,
    };

    use super::LSMStore;

    async fn setup_db(memtable_max_bytes: usize) -> Result<LSMStore> {
        let data_dir = env::temp_dir().join(Uuid::new_v4().to_string());
        DirBuilder::new().create(data_dir.as_path()).await?;
        Ok(LSMStore::new(
            data_dir.as_path(),
            data_dir.join("commit_log").as_path(),
            memtable_max_bytes,
        ))
    }

    #[tokio::test]
    async fn test_end_to_end() -> Result<()> {
        let mut store = self::setup_db(1).await?;
        store.initialize().await?;
        store
            .transact(Transaction::with_random_id(vec![Operation::set(
                "foo", b"foobar",
            )]))
            .await?;
        assert_eq!(
            b"foobar".to_vec(),
            store.get("foo").await?.expect("Could not find key")
        );
        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert_eq!(
            b"foobar".to_vec(),
            store.get("foo").await?.expect("Could not find key")
        );
        let mut sst_file_count = 0;
        let mut dir = tokio::fs::read_dir(store.data_dir.as_path()).await?;
        while let Some(entry) = dir.next_entry().await? {
            if let Some(ext) = entry.path().extension() {
                if ext == "sst" {
                    sst_file_count += 1
                };
            };
        }
        assert_eq!(1, sst_file_count);
        assert_eq!(sst_file_count, store.get_sstables().await?.len());
        Ok(())
    }
}
