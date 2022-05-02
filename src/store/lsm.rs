mod commit_log;
mod sstable;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bloom_filter_rs::{BloomFilter, Murmur3};
use rb_tree::RBMap;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::sync::RwLock;

use self::commit_log::CommitLog;
use self::sstable::SSTable;
use self::Value::{Data, Tombstone};

use super::Operation::{Delete, Set};
use super::{Store, Transaction};
use crate::{utils, Config};
use crate::Result;

/// A store backed by a [log-structured merge tree](http://www.benstopford.com/2015/02/14/log-structured-merge-trees)
pub struct LSMStore {
    commit_log: CommitLog,
    data_dir: PathBuf,
    memtable_max_bytes: u64,
    data: Arc<RwLock<LSMStoreData>>,
}

struct LSMStoreData {
    memtable: RBMap<String, Value>,
    bloom_filter: BloomFilter<Murmur3>,
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
    pub async fn initialize(config: Config, commit_log_path: &Path, data_dir: &Path) -> Result<Self> {
        let commit_log = CommitLog::initialize(commit_log_path).await?;
        let mut store = Self {
            commit_log,
            data_dir: data_dir.to_path_buf(),
            memtable_max_bytes: config.memtable_max_mb *  1_000_000,
            data: Arc::new(RwLock::new(LSMStoreData {
                memtable: RBMap::new(),
                // TODO what is the optimal number of items for the bloom filter?
                bloom_filter: BloomFilter::optimal(Murmur3, 512, 0.01),
            })),
        };
        tokio::spawn(async {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                if store.should_flush_memtable().await.expect("Failed to size memtable") {
                    store.write_sstable().await.expect("Failed to flush memtable");
                };
            };
        });
        Ok(store)
    }

    /// Whether the memtable has grown big enough to flush to disk.
    async fn should_flush_memtable(&self) -> Result<bool> {
        let store = self.data.read().await;
        let size = bincode::serialized_size(&store.memtable)?;
        return Ok(size >= self.memtable_max_bytes)
    }

    /// Returns a vector of SSTable paths, ordered from newest to oldest.
    async fn get_sstables(&self) -> Result<Vec<PathBuf>> {
        let mut sstables = Vec::new();
        let mut dir = fs::read_dir(&self.data_dir).await?;
        while let Some(file) = dir.next_entry().await? {
            sstables.push(file.path());
        }
        sstables.sort_by(|a, b| b.cmp(a));
        Ok(sstables)
    }

    async fn search_sstables(&self, key: &str) -> Result<Option<Value>> {
        for path in self.get_sstables().await? {
            let sstable = SSTable::new(path);
            let v = sstable.search(key.to_owned()).await?;
            if v.is_some() {
                return Ok(v);
            };
        }
        Ok(None)
    }

    /// Writes the current memtable to disk as an SStable then clears
    /// the memtable.
    async fn write_sstable(&self) -> Result<()> {
        let mut store = self.data.write().await;
        let path = self
            .data_dir
            .join(format!("{}.sst", utils::time_since_epoch().as_millis()));
        let sstable = SSTable::new(path);
        sstable.write(&store.memtable).await?;
        store.memtable = RBMap::new();
        Ok(())
    }

    // TODO implement memtable flushes to disk
    // TODO implement segment compaction
}

#[async_trait]
impl Store for LSMStore {
    async fn get(&mut self, k: &str) -> Result<Option<Vec<u8>>> {
        let store = self.data.read().await;
        if !store.bloom_filter.contains(k.as_bytes()) {
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
        self.commit_log.begin_transaction(&transaction).await?;
        let mut store = self.data.write().await;
        for instruction in transaction.operations {
            match instruction {
                Set(key, value) => {
                    store
                        .memtable
                        .insert(key.to_string(), Value::Data(value.to_vec()));
                    store.bloom_filter.insert(key.as_bytes());
                }
                Delete(key) => {
                    store.memtable.insert(key.to_string(), Value::Tombstone);
                }
            };
        }
        Ok(())
    }
}
