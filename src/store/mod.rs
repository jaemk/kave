pub mod lsm;

use self::TransactInstruction::{Delete, Set};
use crate::Result;
use async_trait::async_trait;
use cached::{stores::SizedCache, Cached};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Deserialize, Serialize)]
pub enum TransactInstruction<'a>
{
    Set(&'a str, &'a [u8]),
    Delete(&'a str),
}

#[derive(Deserialize, Serialize)]
pub struct Transaction<'a>
{
    id: Uuid,
    #[serde(borrow)]
    instructions: Vec<TransactInstruction<'a>>,
}

impl<'a> Transaction<'a>
{
    pub fn new(id: Uuid, instructions: Vec<TransactInstruction<'a>>) -> Self {
        Self { id, instructions }
    }

    pub fn with_random_id(instructions: Vec<TransactInstruction<'a>>) -> Self {
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
    async fn transact<'a>(
        &mut self,
        transaction: Transaction<'a>,
    ) -> Result<()>;
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
    async fn transact<'a>(
        &mut self,
        transaction: Transaction<'a>,
    ) -> Result<()> {
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
