pub mod lsm;

use self::TransactInstruction::{Delete, Set};
use crate::Result;
use async_trait::async_trait;
use cached::{stores::SizedCache, Cached};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone)]
pub enum TransactInstruction {
    Set(String, Vec<u8>),
    Delete(String),
}

impl TransactInstruction {
    pub fn set<K: Into<String>>(key: K, value: &[u8]) -> Self {
        Set(key.into(), value.to_vec())
    }

    pub fn delete<K: Into<String>>(key: K) -> Self {
        Delete(key.into())
    }
}

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone)]
pub struct Transaction {
    id: Uuid,
    instructions: Vec<TransactInstruction>,
}

impl Transaction {
    pub fn new(id: Uuid, instructions: Vec<TransactInstruction>) -> Self {
        Self { id, instructions }
    }

    pub fn with_random_id(instructions: Vec<TransactInstruction>) -> Self {
        Self {
            id: Uuid::new_v4(),
            instructions,
        }
    }
}

#[async_trait]
pub trait Store {
    async fn get(&mut self, k: &str) -> Result<Option<Vec<u8>>>;
    /// Returns a vec with the previous values of the keys, if any
    async fn transact(&mut self, transaction: Transaction) -> Result<()>;
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
    async fn get(&mut self, k: &str) -> Result<Option<Vec<u8>>> {
        let mut data = self.data.lock().await;
        Ok(data.cache_get(&k.to_string()).cloned())
    }
    async fn transact(&mut self, transaction: Transaction) -> Result<()> {
        let mut data = self.data.lock().await;
        for instruction in transaction.instructions {
            match instruction {
                Set(key, value) => data.cache_set(key.to_string(), value.to_vec()),
                Delete(key) => data.cache_remove(&key.to_string()),
            };
        }
        Ok(())
    }
}
