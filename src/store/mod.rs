use self::TransactInstruction::{Delete, Set};
use crate::Result;
use async_trait::async_trait;
use bloom_filter_rs::{BloomFilter, Murmur3};
use cached::{stores::SizedCache, Cached};
use rbtree::RBTree;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

pub enum TransactInstruction<'a, K>
where
    K: AsRef<str> + Send,
{
    Set(K, &'a [u8]),
    Delete(K),
}

pub struct Transaction<'a, K>
where
    K: AsRef<str> + Send,
{
    id: Uuid,
    instructions: Vec<TransactInstruction<'a, K>>,
}

impl<'a, K> Transaction<'a, K>
where
    K: AsRef<str> + Send,
{
    pub fn new(id: Uuid, instructions: Vec<TransactInstruction<'a, K>>) -> Self {
        Self { id, instructions }
    }

    pub fn with_random_id(instructions: Vec<TransactInstruction<'a, K>>) -> Self {
        Self {
            id: Uuid::new_v4(),
            instructions,
        }
    }
}

#[async_trait]
pub trait Store {
    async fn get<K: AsRef<str> + Send>(&mut self, k: K) -> Result<Option<Vec<u8>>>;
    /// Returns a vec with the previous values of the keys, if any
    async fn transact<'a, K: AsRef<str> + Send>(
        &mut self,
        transaction: Transaction<'a, K>,
    ) -> Result<()>;
}

/// A store backed by a [log-structured merge tree](http://www.benstopford.com/2015/02/14/log-structured-merge-trees)
pub struct LSMStore {
    memtable: RBTree<String, Option<Vec<u8>>>,
    bloom_filter: BloomFilter<Murmur3>,
}

impl LSMStore {
    pub fn new() -> Self {
        Self {
            memtable: RBTree::new(),
            // TODO what is the optimal number of items for the bloom filter?
            bloom_filter: BloomFilter::optimal(Murmur3, 512, 0.01),
        }
    }

    // TODO implement memtable flushes to disk
    // TODO implement segment compaction
}

#[async_trait]
impl Store for LSMStore {
    async fn get<K: AsRef<str> + Send>(&mut self, k: K) -> Result<Option<Vec<u8>>> {
        if !self.bloom_filter.contains(k.as_ref().as_bytes()) {
            return Ok(None);
        }
        let mem_result = self.memtable.get(&k.as_ref().to_string());
        // TODO read from disk if value not found in memtable
        Ok(mem_result.unwrap().as_deref().map(|v| v.to_vec()))
    }

    async fn transact<'a, K: AsRef<str> + Send>(
        &mut self,
        transaction: Transaction<K>,
    ) -> Result<()> {
        // TODO write begin_transaction to WAL
        for instruction in transaction.instructions {
            match instruction {
                Set(key, value) => {
                    self.memtable
                        .insert(key.as_ref().to_string(), Some(value.to_vec()));
                    self.bloom_filter.insert(key.as_ref().as_bytes());
                }
                Delete(key) => {
                    self.memtable.insert(key.as_ref().to_string(), None);
                }
            };
        }
        Ok(())
    }
}

/// A basic in memory store for testing
#[derive(Clone)]
pub struct MemoryStore {
    data: Arc<Mutex<SizedCache<String, Vec<u8>>>>,
}
impl MemoryStore {
    pub fn new(size: usize) -> Self {
        Self {
            data: Arc::new(Mutex::new(SizedCache::with_size(size))),
        }
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn get<K: AsRef<str> + Send>(&mut self, k: K) -> Result<Option<Vec<u8>>> {
        let mut data = self.data.lock().await;
        Ok(data.cache_get(&k.as_ref().to_string()).cloned())
    }
    async fn transact<'a, K: AsRef<str> + Send>(
        &mut self,
        transaction: Transaction<'a, K>,
    ) -> Result<()> {
        let mut data = self.data.lock().await;
        for instruction in transaction.instructions {
            match instruction {
                Set(key, value) => data.cache_set(key.as_ref().to_string(), value.to_vec()),
                Delete(key) => data.cache_remove(&key.as_ref().to_string()),
            };
        }
        Ok(())
    }
}
