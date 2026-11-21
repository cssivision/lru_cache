//! A small LRU cache shared by `dns-resolver`, `http2socks` and `rabbit`.
//!
//! [`Cache`] keeps entries in a `HashMap` and remembers when each was last
//! used. Every access moves the entry to the recently-used end, and inserting
//! into a full cache evicts the entry with the oldest `last_used`.
//!
//! It also carries a TTL:
//!
//! - each access (`get`, `get_ref`, `get_mut`, `entry`) refreshes `last_used`;
//! - a stale entry is dropped as soon as it is looked up, and
//!   [`Cache::remove_expired`] drops every stale entry at once;
//! - [`LRUCache`] is the same cache behind a mutex, with a background task that
//!   runs that sweep on a timer.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

struct Slot<V> {
    value: V,
    last_used: Instant,
}

/// A TTL cache with an LRU eviction policy.
pub struct Cache<K, V> {
    entries: HashMap<K, Slot<V>>,
    ttl: Duration,
    capacity: usize,
}

impl<K, V> Cache<K, V>
where
    K: Hash + Eq + Clone,
{
    /// Builds a cache that drops a stale entry as soon as it is looked up.
    pub fn new(ttl: Duration, capacity: usize) -> Self {
        Cache {
            entries: HashMap::new(),
            ttl,
            capacity: capacity.max(1),
        }
    }

    /// Returns a copy of the value for `key`, marking it as recently used.
    pub fn get(&mut self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        self.get_ref(key).cloned()
    }

    /// Borrows the value for `key`, marking it as recently used.
    ///
    /// Unlike [`Cache::get`] this does not require `V: Clone`.
    pub fn get_ref(&mut self, key: &K) -> Option<&V> {
        if self.is_expired(key) {
            self.entries.remove(key);
            return None;
        }
        let slot = self.entries.get_mut(key)?;
        slot.last_used = Instant::now();
        Some(&slot.value)
    }

    /// Borrows the value for `key` mutably, marking it as recently used.
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if self.is_expired(key) {
            self.entries.remove(key);
            return None;
        }
        let slot = self.entries.get_mut(key)?;
        slot.last_used = Instant::now();
        Some(&mut slot.value)
    }

    /// Inserts `value` for `key`, evicting the least recently used entry when
    /// the cache is full.
    pub fn insert(&mut self, key: K, value: V) {
        if self.is_expired(&key) {
            self.entries.remove(&key);
        }
        if let Some(slot) = self.entries.get_mut(&key) {
            slot.value = value;
            slot.last_used = Instant::now();
            return;
        }
        if self.entries.len() >= self.capacity {
            self.evict_lru();
        }
        self.entries.insert(
            key,
            Slot {
                value,
                last_used: Instant::now(),
            },
        );
    }

    /// Removes `key`, returning its value.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.entries.remove(key).map(|slot| slot.value)
    }

    /// Returns an entry for `key`, creating it with the given closure if it is
    /// missing.
    pub fn entry(&mut self, key: K) -> Entry<'_, K, V> {
        Entry { cache: self, key }
    }

    /// Number of entries, including entries that are stale but not yet swept.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Drops every entry that has not been used within the TTL.
    pub fn remove_expired(&mut self) {
        self.entries
            .retain(|_, slot| slot.last_used.elapsed() < self.ttl);
    }

    fn is_expired(&self, key: &K) -> bool {
        self.entries
            .get(key)
            .is_some_and(|slot| slot.last_used.elapsed() >= self.ttl)
    }

    fn evict_lru(&mut self) {
        if let Some(key) = self
            .entries
            .iter()
            .min_by_key(|(_, slot)| slot.last_used)
            .map(|(key, _)| key.clone())
        {
            self.entries.remove(&key);
        }
    }
}

/// A view into a single [`Cache`] entry.
pub struct Entry<'a, K, V> {
    cache: &'a mut Cache<K, V>,
    key: K,
}

impl<'a, K, V> Entry<'a, K, V>
where
    K: Hash + Eq + Clone,
{
    /// Inserts the value produced by `default` when the entry is vacant and
    /// returns a mutable reference to the stored value.
    pub fn or_insert_with<F>(self, default: F) -> &'a mut V
    where
        F: FnOnce() -> V,
    {
        let Entry { cache, key } = self;
        if cache.is_expired(&key) {
            cache.entries.remove(&key);
        }
        if !cache.entries.contains_key(&key) {
            if cache.entries.len() >= cache.capacity {
                cache.evict_lru();
            }
            cache.entries.insert(
                key.clone(),
                Slot {
                    value: default(),
                    last_used: Instant::now(),
                },
            );
        }
        let slot = cache
            .entries
            .get_mut(&key)
            .expect("entry was just inserted");
        slot.last_used = Instant::now();
        &mut slot.value
    }
}

/// A [`Cache`] behind a mutex, with a background task that sweeps stale entries.
///
/// The cache is shared with the sweeper through a [`Weak`](std::sync::Weak)
/// reference, so the task stops once the last [`LRUCache`] handle is dropped.
pub struct LRUCache<K, V> {
    inner: Arc<Mutex<Cache<K, V>>>,
}

impl<K, V> Clone for LRUCache<K, V> {
    fn clone(&self) -> Self {
        LRUCache {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, V> LRUCache<K, V>
where
    K: Hash + Eq + Clone + Send + 'static,
    V: Clone + Send + 'static,
{
    /// Builds a cache and spawns the sweeper on the current `awak` runtime.
    pub fn new(ttl: Duration, capacity: usize, cleanup_interval: Duration) -> Self {
        let inner = Arc::new(Mutex::new(Cache::new(ttl, capacity)));
        let weak = Arc::downgrade(&inner);
        awak::spawn(async move {
            loop {
                awak::time::delay_for(cleanup_interval).await;
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                inner.lock().unwrap().remove_expired();
            }
        })
        .detach();
        LRUCache { inner }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.lock().unwrap().get(key)
    }

    pub fn insert(&self, key: K, value: V) {
        self.inner.lock().unwrap().insert(key, value);
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        self.inner.lock().unwrap().remove(key)
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Locks the underlying cache for callers that need more than one operation.
    pub fn lock(&self) -> MutexGuard<'_, Cache<K, V>> {
        self.inner.lock().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache() -> Cache<u16, u16> {
        Cache::new(Duration::from_secs(60), 4)
    }

    #[test]
    fn evicts_least_recently_used() {
        let mut cache = cache();
        cache.insert(1, 1);
        cache.insert(2, 2);
        cache.insert(3, 3);
        cache.insert(4, 4);

        assert!(cache.get_ref(&1).is_some());
        cache.insert(5, 5);

        assert_eq!(cache.len(), 4);
        assert!(cache.get_ref(&1).is_some(), "recent entry evicted");
        assert!(
            cache.get_ref(&2).is_none(),
            "least recently used entry kept"
        );
    }

    #[test]
    fn insert_existing_key_does_not_grow() {
        let mut cache = cache();
        cache.insert(1, 1);
        cache.insert(1, 2);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(&1), Some(2));
    }

    #[test]
    fn access_expiry_drops_stale_entry() {
        let mut cache = Cache::new(Duration::from_millis(5), 4);
        cache.insert(1, 10);
        std::thread::sleep(Duration::from_millis(15));

        assert_eq!(cache.get(&1), None);
        assert!(cache.is_empty());
    }

    #[test]
    fn remove_expired_drops_stale_entry() {
        let mut cache = Cache::new(Duration::from_millis(5), 4);
        cache.insert(1, 10);
        std::thread::sleep(Duration::from_millis(15));

        cache.remove_expired();
        assert!(cache.is_empty());
    }

    #[test]
    fn entry_inserts_once_and_refreshes() {
        let mut cache = cache();
        *cache.entry(7).or_insert_with(|| 1) += 1;
        *cache.entry(7).or_insert_with(|| 1) += 1;

        assert_eq!(cache.get(&7), Some(3));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn remove_returns_value_and_reuses_slot() {
        let mut cache = cache();
        cache.insert(9, 9);
        assert_eq!(cache.remove(&9), Some(9));
        assert_eq!(cache.remove(&9), None);

        cache.insert(10, 10);
        assert_eq!(cache.get(&10), Some(10));
        assert_eq!(cache.len(), 1);
    }
}
