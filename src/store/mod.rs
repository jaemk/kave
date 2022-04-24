use crate::Result;
use async_trait::async_trait;
use cached::{stores::SizedCache, Cached};
use std::sync::Arc;
use tokio::sync::Mutex;

#[async_trait]
pub trait Store {
    async fn get<K: AsRef<str> + Send>(&mut self, k: K) -> Result<Option<Vec<u8>>>;
    async fn set<K: AsRef<str> + Send>(&mut self, k: K, v: &[u8]) -> Result<Option<Vec<u8>>>;
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
unsafe impl Send for MemoryStore {}
unsafe impl Sync for MemoryStore {}

#[async_trait]
impl Store for MemoryStore {
    async fn get<K: AsRef<str> + Send>(&mut self, k: K) -> Result<Option<Vec<u8>>> {
        let mut data = self.data.lock().await;
        Ok(data.cache_get(&k.as_ref().to_string()).cloned())
    }
    async fn set<K: AsRef<str> + Send>(&mut self, k: K, v: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut data = self.data.lock().await;
        Ok(data.cache_set(k.as_ref().to_string(), v.to_vec()))
    }
}
