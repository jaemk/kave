use crate::Result;
use async_trait::async_trait;
use cached::{stores::SizedCache, Cached};
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
    async fn transact<'a, K: AsRef<str> + Send>(
        &mut self,
        transaction: Transaction<'a, K>,
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
                TransactInstruction::Set(key, value) => {
                    data.cache_set(key.as_ref().to_string(), value.to_vec())
                }
                TransactInstruction::Delete(key) => data.cache_remove(&key.as_ref().to_string()),
            };
        }
        Ok(())
    }
}
