//! Persistent disk storage
pub mod lsm;

use self::Operation::{Delete, Set};
use crate::Result;
use async_trait::async_trait;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone)]
pub enum Operation {
    Set(String, Vec<u8>),
    Delete(String),
}

impl Operation {
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
    operations: Vec<Operation>,
}

impl Transaction {
    pub fn new(id: Uuid, operations: Vec<Operation>) -> Self {
        Self { id, operations }
    }

    pub fn with_random_id(operations: Vec<Operation>) -> Self {
        Self {
            id: Uuid::new_v4(),
            operations,
        }
    }
}

#[async_trait]
pub trait Store {
    async fn get(&mut self, k: &str) -> Result<Option<Vec<u8>>>;
    /// Returns all values with keys from `from` (inclusive) to `to` (exclusive).
    async fn scan(&mut self, from_inclusive: &str, to_exclusive: &str) -> Result<Vec<Vec<u8>>>;
    async fn transact(&mut self, transaction: Transaction) -> Result<()>;
    async fn shutdown(&mut self) -> Result<()>;
}

/// A basic in memory store for testing
#[derive(Clone, Default)]
pub struct MemoryStore {
    data: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
}
impl MemoryStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn get(&mut self, k: &str) -> Result<Option<Vec<u8>>> {
        let data = self.data.lock().await;
        Ok(data.get(&k.to_string()).cloned())
    }

    async fn scan(&mut self, from_inclusive: &str, to_exclusive: &str) -> Result<Vec<Vec<u8>>> {
        let data = self.data.lock().await;
        let result = data
            .range(from_inclusive.to_string()..to_exclusive.to_string())
            .into_iter()
            .map(|(_, v)| v.to_owned())
            .collect_vec();
        Ok(result)
    }

    async fn transact(&mut self, transaction: Transaction) -> Result<()> {
        let mut data = self.data.lock().await;
        for instruction in transaction.operations {
            match instruction {
                Set(key, value) => data.insert(key.to_string(), value.to_vec()),
                Delete(key) => data.remove(&key.to_string()),
            };
        }
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}
