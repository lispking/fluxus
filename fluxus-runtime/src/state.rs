use parking_lot::RwLock;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

/// Simple key-value state backend
#[derive(Default)]
pub struct KeyedStateBackend<K, V> {
    state: Arc<RwLock<HashMap<K, V>>>,
}

impl<K, V> KeyedStateBackend<K, V>
where
    K: Eq + Hash,
{
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        self.state.read().get(key).cloned()
    }

    pub fn set(&self, key: K, value: V) {
        self.state.write().insert(key, value);
    }
}
