use crate::storage::StorageProvider;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Default)]
pub struct MockProvider {
    inner: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl MockProvider {
    pub fn new() -> Self {
        MockProvider {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn into_arc(self) -> Arc<Self> {
        Arc::new(self)
    }
}

#[async_trait]
impl StorageProvider for MockProvider {
    async fn put_object(
        &self,
        key: &str,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut m = self.inner.lock().await;
        m.insert(key.to_string(), data);
        Ok(())
    }

    async fn get_object(
        &self,
        key: &str,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        let m = self.inner.lock().await;
        Ok(m.get(key).cloned())
    }

    async fn list_objects(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let m = self.inner.lock().await;
        let mut v = Vec::new();
        for k in m.keys() {
            if k.starts_with(prefix) {
                v.push(k.clone());
            }
        }
        Ok(v)
    }

    async fn delete_object(
        &self,
        key: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut m = self.inner.lock().await;
        m.remove(key);
        Ok(())
    }
}
