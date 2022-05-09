//! [Log-structured merge tree](http://www.benstopford.com/2015/02/14/log-structured-merge-trees) implementation
mod commit_log;
mod sstable;

use std::collections::{BTreeMap, HashMap};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use growable_bloom_filter::GrowableBloom;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

use self::commit_log::CommitLog;
use self::sstable::SSTable;
use self::Value::{Data, Tombstone};

use super::Operation::{Delete, Set};
use super::{Store, Transaction};
use crate::Result;
use crate::{utils, Config};

type Shared<T> = Arc<RwLock<T>>;

// TODO what are the optimal values for these bloom filter parameters?
const BLOOM_ERROR_PROB: f64 = 0.01;
const BLOOM_EST_INSERTIONS: usize = 128;

/// A store backed by a [log-structured merge tree](http://www.benstopford.com/2015/02/14/log-structured-merge-trees).
#[derive(Clone)]
pub struct LSMStore {
    data: Shared<LSMData>,
    commit_log: Shared<CommitLog>,
    data_dir: PathBuf,
    memtable_max_bytes: usize,
    bloom_map: Shared<HashMap<PathBuf, GrowableBloom>>,
    bloom_map_path: PathBuf,
    event_sender: broadcast::Sender<LSMEvent>,
}

struct LSMData {
    memtable: BTreeMap<String, Value>,
    tx_ids: Vec<Uuid>,
}

/// Events published on the events channel exposed by the store
#[derive(Clone, Debug)]
pub enum LSMEvent {
    WriteSSTable(PathBuf),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Data(Vec<u8>),
    Tombstone,
}

impl Value {
    fn as_option(&self) -> Option<Vec<u8>> {
        match self {
            Data(data) => Some(data.to_vec()),
            Tombstone => None,
        }
    }
}

impl LSMStore {
    fn new(data_dir: &Path, commit_log_path: &Path, memtable_max_bytes: usize) -> Self {
        let commit_log = CommitLog::new(commit_log_path);
        let (event_tx, _) = broadcast::channel(8);
        Self {
            data: Arc::new(RwLock::new(LSMData {
                memtable: BTreeMap::new(),
                tx_ids: Vec::new(),
            })),
            commit_log: Arc::new(RwLock::new(commit_log)),
            data_dir: data_dir.to_path_buf(),
            memtable_max_bytes,
            bloom_map: Arc::new(RwLock::new(HashMap::new())),
            bloom_map_path: data_dir.join("bloom_map"),
            event_sender: event_tx,
        }
    }

    fn from_config(config: &Config) -> Self {
        Self::new(
            config.data_dir.as_path(),
            config.commit_log_path.as_path(),
            config.memtable_max_mb * 1_000_000,
        )
    }

    pub fn events(&mut self) -> broadcast::Receiver<LSMEvent> {
        self.event_sender.subscribe()
    }

    pub async fn initialize_from_config(config: &Config) -> Result<Self> {
        let mut store = Self::from_config(config);
        store.initialize().await?;
        Ok(store)
    }

    async fn initialize(&mut self) -> Result<()> {
        self.restore_bloom_map().await?;
        self.restore_previous_txs().await?;
        self.start_background_tasks();
        Ok(())
    }

    async fn restore_bloom_map(&mut self) -> Result<()> {
        let bloom_map = self.restore_bloom_map_from_file().await?;
        if let Some(bloom) = bloom_map {
            tracing::debug!(
                path = ?self.bloom_map_path.as_path(),
                "Restored bloom map from file"
            );
            self.bloom_map = Arc::new(RwLock::new(bloom));
        } else {
            let bloom_map = self.reconstruct_bloom_map_from_sstables().await?;
            tracing::debug!("Reconstructed bloom map from SSTables");
            self.bloom_map = Arc::new(RwLock::new(bloom_map));
        }
        Ok(())
    }

    /// Reconstructs the bloom filter map by iterating over all SSTable keys.
    async fn reconstruct_bloom_map_from_sstables(&self) -> Result<HashMap<PathBuf, GrowableBloom>> {
        let mut bloom_map = HashMap::new();
        for path in self.get_sstables_asc().await? {
            let sstable = SSTable::new(path.as_path());
            let keys = sstable.keys().await?;
            let mut bloom = GrowableBloom::new(BLOOM_ERROR_PROB, BLOOM_EST_INSERTIONS);
            for key in keys {
                bloom.insert(key);
            }
            bloom_map.insert(path, bloom);
        }
        Ok(bloom_map)
    }

    async fn restore_previous_txs(&mut self) -> Result<()> {
        tracing::debug!("Looking for unfinished transactions...");
        let commit_log_ref = self.commit_log.clone();
        let commit_log = commit_log_ref.read().await;
        for tx in commit_log.get_unfinished_transactions().await? {
            tracing::debug!(tx_id = ?tx.id, "Restoring transaction");
            self.do_transact(tx, false).await?;
        }
        Ok(())
    }

    fn start_background_tasks(&self) {
        let data = self.data.clone();
        let data_dir = self.data_dir.clone();
        let bloom_map = self.bloom_map.clone();
        let commit_log = self.commit_log.clone();
        let memtable_max_bytes = self.memtable_max_bytes;
        let event_sender = self.event_sender.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                if Self::should_flush_memtable(data.clone(), memtable_max_bytes)
                    .await
                    .expect("Failed to size memtable")
                {
                    tracing::debug!("Flushing memtable to disk...");
                    if let Some(path) = Self::write_sstable(
                        data.clone(),
                        data_dir.clone().as_path(),
                        bloom_map.clone(),
                        commit_log.clone(),
                    )
                    .await
                    .expect("Failed to flush memtable")
                    {
                        event_sender
                            .send(LSMEvent::WriteSSTable(path))
                            .expect("Failed to send memtable flush event");
                        tracing::debug!("Flushed memtable");
                    }
                };
            }
        });
        // TODO implement segment compaction
    }

    /// Whether the memtable has grown big enough to flush to disk.
    async fn should_flush_memtable(
        shared_data: Shared<LSMData>,
        memtable_max_bytes: usize,
    ) -> Result<bool> {
        let data = shared_data.read().await;
        Ok(!data.memtable.is_empty() && mem::size_of_val(&data.memtable) >= memtable_max_bytes)
    }

    /// Returns a vector of SSTable paths, ordered from oldest to newest.
    async fn get_sstables_asc(&self) -> Result<Vec<PathBuf>> {
        let mut sstables = Vec::new();
        let mut dir = fs::read_dir(&self.data_dir).await?;
        while let Some(file) = dir.next_entry().await? {
            if let Some(ext) = file.path().extension() {
                if ext == "sst" {
                    sstables.push(file.path());
                };
            };
        }
        sstables.sort();
        Ok(sstables)
    }

    async fn sstables_for_key(&self, key: &str) -> Vec<PathBuf> {
        let key = key.to_string();
        let bloom_map = self.bloom_map.read().await;
        bloom_map
            .iter()
            .filter(|(_, bloom)| bloom.contains(&key))
            .map(|(path, _)| path.clone())
            .sorted_by(|a, b| b.cmp(a))
            .collect()
    }

    async fn search_sstables(&self, key: &str) -> Result<Option<Value>> {
        for path in self.sstables_for_key(key).await {
            let sstable = SSTable::new(&path);
            let v = sstable.search(key.to_owned()).await?;
            if v.is_some() {
                return Ok(v);
            };
        }
        Ok(None)
    }

    async fn scan_sstables(
        &self,
        from_inclusive: &str,
        to_exclusive: &str,
    ) -> Result<Vec<(String, Value)>> {
        let mut scan_kvs = BTreeMap::new();
        for path in self.get_sstables_asc().await? {
            let sstable = SSTable::new(&path);
            for (k, v) in sstable.scan(from_inclusive, to_exclusive).await? {
                scan_kvs.insert(k, v);
            }
        }
        Ok(scan_kvs.into_iter().collect())
    }

    /// Writes the current memtable to disk as an SStable then clears
    /// the memtable.
    async fn write_sstable(
        shared_data: Shared<LSMData>,
        data_dir: &Path,
        bloom_map: Shared<HashMap<PathBuf, GrowableBloom>>,
        commit_log: Shared<CommitLog>,
    ) -> Result<Option<PathBuf>> {
        let mut data = shared_data.write().await;
        if data.memtable.is_empty() {
            return Ok(None);
        }
        let mut bloom_map = bloom_map.write().await;
        let path = data_dir.join(format!("{}.sst", utils::time_since_epoch().as_millis()));
        let sstable = SSTable::new(path.clone());
        sstable.write(&data.memtable).await?;
        tracing::debug!(
            path = ?path.as_path(),
            "Wrote SSTable file"
        );
        bloom_map.insert(
            path.clone(),
            GrowableBloom::new(BLOOM_ERROR_PROB, BLOOM_EST_INSERTIONS),
        );
        let keys = data.memtable.keys().clone();
        for key in keys {
            let bloom = bloom_map.get_mut(&path).unwrap();
            bloom.insert(key.clone());
        }
        data.memtable = BTreeMap::new();
        let mut commit_log = commit_log.write().await;
        for tx_id in &data.tx_ids {
            commit_log.end_transaction(tx_id).await?;
        }
        data.tx_ids = Vec::new();
        Ok(Some(path))
    }

    /// Write the bloom filter to disk for later recovery.
    async fn write_bloom_map(
        bloom_map: Shared<HashMap<PathBuf, GrowableBloom>>,
        bloom_path: &Path,
    ) -> Result<()> {
        let bloom_map = bloom_map.read().await;
        if bloom_map.is_empty() {
            return Ok(());
        }
        let buf = bincode::serialize(&*bloom_map)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(bloom_path)
            .await?;
        file.write_all(buf.as_slice()).await?;
        file.sync_all().await?;
        tracing::debug!(path = ?bloom_path, "Wrote bloom map file");
        Ok(())
    }

    /// Restore the bloom filter map from disk if it is up-to-date
    /// with the commit log.
    async fn restore_bloom_map_from_file(
        &mut self,
    ) -> Result<Option<HashMap<PathBuf, GrowableBloom>>> {
        let path = &self.bloom_map_path;
        if !path.exists() {
            return Ok(None);
        }
        let commit_log = self.commit_log.read().await;
        let commit_log_mod = fs::metadata(commit_log.path()).await?.modified()?;
        let filter_file_mod = fs::metadata(path.as_path()).await?.modified()?;
        if commit_log_mod > filter_file_mod {
            return Ok(None);
        }
        let mut file = OpenOptions::new().read(true).open(path).await?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;
        let bloom_map = bincode::deserialize(buf.as_slice())?;
        Ok(Some(bloom_map))
    }

    async fn do_transact(&mut self, transaction: Transaction, log_commit: bool) -> Result<()> {
        if log_commit {
            let mut commit_log = self.commit_log.write().await;
            commit_log.begin_transaction(&transaction).await?;
        }
        let mut data = self.data.write().await;
        let tx_ids = &mut data.tx_ids;
        tx_ids.push(transaction.id);
        for instruction in transaction.operations {
            match instruction {
                Set(key, value) => {
                    data.memtable
                        .insert(key.to_string(), Value::Data(value.to_vec()));
                }
                Delete(key) => {
                    data.memtable.insert(key.to_string(), Value::Tombstone);
                }
            };
        }
        Ok(())
    }
}

#[async_trait]
impl Store for LSMStore {
    async fn get(&mut self, k: &str) -> Result<Option<Vec<u8>>> {
        let store = self.data.read().await;
        let mut result = store
            .memtable
            .get(&k.to_string())
            .and_then(|v| v.as_option());
        if result.is_none() {
            result = self.search_sstables(k).await?.and_then(|v| v.as_option());
        }
        Ok(result)
    }

    async fn scan(&mut self, from_inclusive: &str, to_exclusive: &str) -> Result<Vec<Vec<u8>>> {
        let store = self.data.read().await;
        let mut scan_result = BTreeMap::new();
        for (k, v) in self.scan_sstables(from_inclusive, to_exclusive).await? {
            scan_result.insert(k, v);
        }
        for (k, v) in store
            .memtable
            .range(from_inclusive.to_string()..to_exclusive.to_string())
        {
            scan_result.insert(k.to_owned(), v.to_owned());
        }
        Ok(scan_result
            .iter()
            .filter_map(|(_, v)| match v.clone() {
                Data(data) => Some(data),
                Tombstone => None,
            })
            .collect())
    }

    async fn transact(&mut self, transaction: Transaction) -> Result<()> {
        self.do_transact(transaction, true).await
    }

    async fn shutdown(&mut self) -> Result<()> {
        let data = self.data.clone();
        let commit_log = self.commit_log.clone();
        let bloom_map = self.bloom_map.clone();
        Self::write_sstable(
            data.clone(),
            self.data_dir.as_path(),
            bloom_map.clone(),
            commit_log,
        )
        .await?;
        Self::write_bloom_map(bloom_map.clone(), self.bloom_map_path.as_path()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        path::{Path, PathBuf},
        time::Duration,
    };

    use assert_matches::assert_matches;
    use tokio::{fs::DirBuilder, time::timeout};
    use uuid::Uuid;

    use crate::{
        store::{Operation, Store, Transaction},
        Result,
    };

    use super::{LSMEvent, LSMStore};

    async fn test_data_dir() -> Result<PathBuf> {
        let data_dir = env::temp_dir().join(Uuid::new_v4().to_string());
        DirBuilder::new().create(data_dir.as_path()).await?;
        Ok(data_dir)
    }

    fn setup_db(data_dir: &Path, memtable_max_bytes: usize) -> LSMStore {
        LSMStore::new(
            data_dir,
            data_dir.join("commit_log").as_path(),
            memtable_max_bytes,
        )
    }

    #[tokio::test]
    async fn test_end_to_end() -> Result<()> {
        let data_dir = self::test_data_dir().await?;
        let mut store = self::setup_db(data_dir.as_path(), 1);
        store.initialize().await?;
        let mut events = store.events();
        store
            .transact(Transaction::with_random_id(vec![Operation::set(
                "foo", b"foobar",
            )]))
            .await?;
        assert_eq!(
            b"foobar".to_vec(),
            store.get("foo").await?.expect("Could not find key")
        );
        let LSMEvent::WriteSSTable(sstable_path) = timeout(Duration::from_secs(2), events.recv())
            .await?
            .expect("Error receiving event from LSM store");
        assert_eq!(
            b"foobar".to_vec(),
            store.get("foo").await?.expect("Could not find key")
        );
        let mut dir = tokio::fs::read_dir(store.data_dir.as_path()).await?;
        let mut sst_files = Vec::new();
        while let Some(entry) = dir.next_entry().await? {
            if let Some(ext) = entry.path().extension() {
                if ext == "sst" {
                    sst_files.push(entry.path());
                };
            };
        }
        assert_eq!(vec![sstable_path], sst_files);
        assert_eq!(sst_files, store.get_sstables_asc().await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_crash_recovery() -> Result<()> {
        let data_dir = self::test_data_dir().await?;
        {
            let mut store = self::setup_db(data_dir.as_path(), 1000);
            store
                .transact(Transaction::with_random_id(vec![
                    (Operation::set("foo", b"bar")),
                ]))
                .await?;
        }
        let mut store = self::setup_db(data_dir.as_path(), 1000);
        store.initialize().await?;
        assert_eq!(
            b"bar".to_vec(),
            store.get("foo").await?.expect("Could not find key")
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_shutdown_recovery() -> Result<()> {
        let data_dir = self::test_data_dir().await?;
        {
            let mut store = self::setup_db(data_dir.as_path(), 1000);
            store
                .transact(Transaction::with_random_id(vec![
                    (Operation::set("foo", b"bar")),
                ]))
                .await?;
            store.shutdown().await?;
        }
        let mut store = self::setup_db(data_dir.as_path(), 1000);
        store.initialize().await?;
        assert_eq!(
            b"bar".to_vec(),
            store.get("foo").await?.expect("Could not find key")
        );
        let reconstructed_bloom = store.reconstruct_bloom_map_from_sstables().await?;
        assert_eq!(1, reconstructed_bloom.len());
        Ok(())
    }

    #[tokio::test]
    async fn test_scan() -> Result<()> {
        let data_dir = self::test_data_dir().await?;
        let mut store = self::setup_db(data_dir.as_path(), 1);
        store.initialize().await?;
        let mut events = store.events();
        store
            .transact(Transaction::with_random_id(vec![
                Operation::set("a", b"first"),
                Operation::set("b", b"second"),
                Operation::set("c", b"third"),
                Operation::set("d", b"fourth"),
                Operation::set("e", b"fifth"),
            ]))
            .await?;
        assert_eq!(
            vec![b"second".to_vec(), b"third".to_vec(), b"fourth".to_vec()],
            store.scan("b", "dd").await?
        );
        let empty: Vec<Vec<u8>> = vec![];
        assert_eq!(empty, store.scan("ee", "f").await?);
        assert_eq!(
            vec![b"fourth".to_vec(), b"fifth".to_vec()],
            store.scan("cc", "z").await?
        );
        let event = timeout(Duration::from_secs(2), events.recv())
            .await?
            .expect("Error receiving event from LSM store");
        assert_matches!(event, LSMEvent::WriteSSTable(_));
        assert_eq!(
            vec![b"second".to_vec(), b"third".to_vec(), b"fourth".to_vec()],
            store.scan("b", "dd").await?
        );
        store
            .transact(Transaction::with_random_id(vec![
                Operation::set("0", b"zeroth"),
                Operation::set("f", b"sixth"),
            ]))
            .await?;
        let event = timeout(Duration::from_secs(2), events.recv())
            .await?
            .expect("Error receiving event from LSM store");
        assert_matches!(event, LSMEvent::WriteSSTable(_));
        assert_eq!(
            vec![
                b"zeroth".to_vec(),
                b"first".to_vec(),
                b"second".to_vec(),
                b"third".to_vec(),
                b"fourth".to_vec(),
                b"fifth".to_vec(),
                b"sixth".to_vec()
            ],
            store.scan("0", "z").await?
        );
        Ok(())
    }
}
