# lru_cache

A small LRU cache with a TTL.

- O(1) `get`, `get_ref`, `get_mut`, `remove` and `entry`; `insert` is O(1) until
  the cache is full, when eviction scans for the least recently used entry.
- Access moves an entry to the most-recently-used end; inserting into a full
  cache evicts the least recently used entry.
- Each entry has a TTL, refreshed on every access. A stale entry is dropped as
  soon as it is looked up.
- `LRUCache` wraps the cache in a `Mutex` and sweeps stale entries from a
  background task.

## Usage

```rust
use std::time::Duration;
use lru_cache::Cache;

let mut cache = Cache::new(Duration::from_secs(600), 1024);

cache.insert("a", 1);
assert_eq!(cache.get(&"a"), Some(1));

// Look up or create, then mutate in place.
*cache.entry("a").or_insert_with(|| 0) += 1;
assert_eq!(cache.get(&"a"), Some(2));
```

For a shared cache with background expiry (spawned on the current `awak`
runtime):

```rust
use std::time::Duration;
use lru_cache::LRUCache;

let cache = LRUCache::new(
    Duration::from_secs(600), // TTL
    1024,                     // capacity
    Duration::from_secs(60),  // sweep interval
);

cache.insert("a", 1);
assert_eq!(cache.get(&"a"), Some(1));
```

## API

### `Cache<K, V>`

| Method | Description |
| --- | --- |
| `new(ttl, capacity)` | Build a cache. Capacity is clamped to at least 1. |
| `get(&key) -> Option<V>` | Clone the value and mark it as recently used. |
| `get_ref(&key) -> Option<&V>` | Borrow the value without requiring `V: Clone`. |
| `get_mut(&key) -> Option<&mut V>` | Mutable borrow, marked as recently used. |
| `insert(key, value)` | Insert or update; evicts the LRU entry when full. |
| `remove(&key) -> Option<V>` | Remove and return a value. |
| `entry(key) -> Entry` | `or_insert_with(closure)` returns `&mut V`, creating it if missing. |
| `len()` / `is_empty()` | Number of entries, including not-yet-swept stale ones. |
| `remove_expired()` | Drop every entry older than the TTL. |

Stale entries are removed on lookup, so callers never observe an expired
entry. Only `get` requires `V: Clone`; keys and values are otherwise not
required to be `Clone`.

### `LRUCache<K, V>`

`LRUCache` is a `Clone` handle around `Arc<Mutex<Cache<K, V>>>`. It provides
`new(ttl, capacity, cleanup_interval)`, which spawns the sweeper, plus `get`,
`insert`, `remove`, `len`, `is_empty` and `lock()` for multi-step operations.

## License

MIT
