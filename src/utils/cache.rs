use crate::middlewares::CallResult;
use blake2::{digest::Output, Digest};
use futures::future::BoxFuture;
use jsonrpsee::core::JsonValue;
use jsonrpsee::types::ErrorObjectOwned;
use std::num::NonZeroUsize;
use std::time::Duration;
use tokio::sync::watch;

#[derive(Debug)]
pub struct CacheKey<D: Digest>(pub Output<D>);

impl<D: Digest> Clone for CacheKey<D> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<D: Digest> CacheKey<D> {
    /// The key for `method` called with `params`.
    ///
    /// Every part is hashed with its length in front. Hashing the serialized
    /// params one after another with nothing between them made different
    /// calls share a key whenever their params concatenated to the same text:
    /// `[1, 23]` and `[12, 3]` both hashed "123", so the second caller was
    /// served the first caller's cached answer.
    pub fn new(method: &String, params: &[JsonValue]) -> Self {
        let mut hasher = D::new();
        let mut part = |bytes: &[u8]| {
            hasher.update((bytes.len() as u64).to_le_bytes());
            hasher.update(bytes);
        };
        part(method.as_bytes());
        for p in params {
            part(p.to_string().as_bytes());
        }

        Self(hasher.finalize())
    }
}

impl<D: Digest> PartialEq for CacheKey<D> {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_slice() == other.0.as_slice()
    }
}

impl<D: Digest> Eq for CacheKey<D> {}

impl<D: Digest> std::hash::Hash for CacheKey<D> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.as_slice().hash(state);
    }
}

#[derive(Clone, Debug)]
pub enum CacheValue {
    Pending(watch::Receiver<Option<Result<JsonValue, ErrorObjectOwned>>>),
    Value(JsonValue),
}

#[derive(Clone)]
pub struct Cache<D: Digest> {
    cache: moka::future::Cache<CacheKey<D>, CacheValue>,
}

impl<D: Digest + 'static> Cache<D> {
    pub fn new(size: NonZeroUsize, ttl: Option<Duration>) -> Self {
        let size = size.get();
        let mut builder = moka::future::Cache::<CacheKey<D>, CacheValue>::builder()
            .max_capacity(size as u64)
            .initial_capacity(size);

        if let Some(duration) = ttl {
            builder = builder.time_to_live(duration);
        }

        let cache = builder.build();

        Self { cache }
    }

    pub async fn get(&self, key: &CacheKey<D>) -> Option<JsonValue> {
        match self.cache.get(key).await {
            Some(CacheValue::Value(value)) => Some(value),
            Some(CacheValue::Pending(mut rx)) => {
                let value = rx.borrow();
                if value.is_some() {
                    return value.clone().unwrap().ok();
                }
                drop(value);
                let _ = rx.changed().await;
                let value = rx.borrow();
                if value.is_some() {
                    value.clone().unwrap().ok()
                } else {
                    tracing::error!("Cache: Unreachable code");
                    None
                }
            }
            None => None,
        }
    }

    pub async fn insert(&self, key: CacheKey<D>, value: JsonValue) {
        self.cache.insert(key, CacheValue::Value(value)).await;
    }

    pub async fn get_or_insert_with<F>(&self, key: CacheKey<D>, f: F) -> CallResult
    where
        F: FnOnce() -> BoxFuture<'static, CallResult>,
    {
        let fetch = || async {
            let (tx, rx) = watch::channel(None);
            self.cache.insert(key.clone(), CacheValue::Pending(rx)).await;
            let value = f().await;
            let _ = tx.send(Some(value.clone()));
            match &value {
                Ok(value) => {
                    self.cache.insert(key.clone(), CacheValue::Value(value.clone())).await;
                }
                Err(_) => {
                    self.cache.remove(&key).await;
                }
            };
            value
        };

        match self.cache.get(&key).await {
            Some(CacheValue::Value(value)) => Ok(value),
            Some(CacheValue::Pending(mut rx)) => {
                {
                    // limit the scope of value
                    let value = rx.borrow();
                    if value.is_some() {
                        return value.clone().unwrap();
                    }
                }

                let _ = rx.changed().await;

                {
                    // limit the scope of value
                    let value = rx.borrow();
                    if let Some(value) = &*value {
                        return value.clone();
                    }
                }

                // this only happens when initial fetch request got canceled for some reason
                // in that case we need to fetch again
                fetch().await
            }
            None => fetch().await,
        }
    }

    pub async fn remove(&self, key: &CacheKey<D>) {
        self.cache.remove(key).await;
    }

    pub async fn sync(&self) {
        self.cache.run_pending_tasks().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt as _;
    use jsonrpsee::types::error::reject_too_big_request;
    use serde_json::json;

    fn key(method: &str, params: JsonValue) -> CacheKey<blake2::Blake2b512> {
        CacheKey::new(&method.to_string(), params.as_array().unwrap())
    }

    #[test]
    fn key_separates_params() {
        // the collision this fixes: both used to hash "m" + "1" + "23" / "12" + "3"
        assert!(key("m", json!([1, 23])) != key("m", json!([12, 3])));
        assert!(key("m", json!([1, 2, 3])) != key("m", json!([12, 3])));
        assert!(key("m", json!([123])) != key("m", json!([1, 23])));
        assert!(key("m", json!([1, 23, "0xab"])) != key("m", json!([12, 3, "0xab"])));
        assert!(key("m", json!([[1], [2]])) != key("m", json!([[1, 2]])));
        assert!(key("m", json!([true, 1])) != key("m", json!(["true1"])));
        // an absent trailing param is not the same call as an explicit null
        assert!(key("m", json!([1])) != key("m", json!([1, null])));
        // the method is separated from the params too
        assert!(key("m1", json!([2])) != key("m", json!([12])));
        assert!(key("ab", json!([])) != key("a", json!(["b"])));
        // and equal calls still share a key
        assert!(key("m", json!([1, 23])) == key("m", json!([1, 23])));
        assert!(key("m", json!([{"a": 1}, "x"])) == key("m", json!([{"a": 1}, "x"])));
    }

    #[tokio::test]
    async fn get_insert_remove() {
        let cache = Cache::<blake2::Blake2b512>::new(NonZeroUsize::new(1).unwrap(), None);

        let key = CacheKey::<blake2::Blake2b512>::new(&"key".to_string(), &[]);

        assert_eq!(cache.get(&key).await, None);

        cache.insert(key.clone(), json!("value")).await;

        assert_eq!(cache.get(&key).await, Some(json!("value")));

        cache.remove(&key).await;

        assert_eq!(cache.get(&key).await, None);
    }

    #[tokio::test]
    async fn get_or_insert_with_basic() {
        let cache = Cache::<blake2::Blake2b512>::new(NonZeroUsize::new(1).unwrap(), None);

        let key = CacheKey::<blake2::Blake2b512>::new(&"key".to_string(), &[]);

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        let cache2 = cache.clone();
        let key2 = key.clone();
        let h1 = tokio::spawn(async move {
            let value = cache2
                .get_or_insert_with(key2.clone(), || {
                    async move {
                        let _ = rx.await;
                        Ok(json!("value"))
                    }
                    .boxed()
                })
                .await;
            assert_eq!(value, Ok(json!("value")));
        });

        tokio::task::yield_now().await;

        let cache2 = cache.clone();
        let key2 = key.clone();
        let h2 = tokio::spawn(async move {
            let value = cache2
                .get_or_insert_with(key2, || {
                    async {
                        panic!();
                    }
                    .boxed()
                })
                .await;
            assert_eq!(value, Ok(json!("value")));
        });

        tokio::task::yield_now().await;

        tx.send(()).unwrap();

        h1.await.unwrap();
        h2.await.unwrap();

        assert_eq!(cache.get(&key).await, Some(json!("value")));
    }

    #[tokio::test]
    async fn get_or_insert_with_handle_canceled_request() {
        let cache = Cache::<blake2::Blake2b512>::new(NonZeroUsize::new(1).unwrap(), None);

        let key = CacheKey::<blake2::Blake2b512>::new(&"key".to_string(), &[]);

        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();

        let cache2 = cache.clone();
        let key2 = key.clone();
        let h1 = tokio::spawn(async move {
            let _ = cache2
                .get_or_insert_with(key2.clone(), || {
                    async move {
                        let _ = rx.await;
                        panic!();
                    }
                    .boxed()
                })
                .await;
            unreachable!();
        });

        tokio::task::yield_now().await;

        let cache2 = cache.clone();
        let key2 = key.clone();
        let h2 = tokio::spawn(async move {
            let value = cache2
                .get_or_insert_with(key2, || async { Ok(json!("value")) }.boxed())
                .await;
            assert_eq!(value, Ok(json!("value")));
        });

        tokio::task::yield_now().await;

        h1.abort(); // first request failed for whatever reason

        h1.await.unwrap_err();
        h2.await.unwrap(); // second request should still work

        assert_eq!(cache.get(&key).await, Some(json!("value")));
    }

    #[tokio::test]
    async fn get_or_insert_with_error() {
        let cache = Cache::<blake2::Blake2b512>::new(NonZeroUsize::new(1).unwrap(), None);

        let key = CacheKey::<blake2::Blake2b512>::new(&"key".to_string(), &[]);

        let value = cache
            .get_or_insert_with(key.clone(), || async move { Err(reject_too_big_request(100)) }.boxed())
            .await;
        assert_eq!(value, Err(reject_too_big_request(100)));

        assert_eq!(cache.get(&key).await, None);
    }
}
