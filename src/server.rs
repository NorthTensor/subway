use std::{net::SocketAddr, sync::Arc};

use futures::FutureExt;
use jsonrpsee::{
    core::JsonValue,
    server::{RpcModule, ServerHandle},
    types::error::INTERNAL_ERROR_CODE,
    types::ErrorObjectOwned,
};
use opentelemetry::trace::FutureExt as _;
use serde_json::json;

use crate::{
    config::{Config, MethodParam},
    extensions::{
        prometheus::get_rpc_metrics,
        rate_limit::{MethodWeights, RateLimitBuilder},
        server::SubwayServerBuilder,
    },
    middlewares::{factory, CallRequest, Middlewares, SubscriptionRequest},
    utils::{by_name, errors, telemetry, TypeRegistryRef},
};

// TODO: https://github.com/paritytech/jsonrpsee/issues/985
fn string_to_static_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

/// Turns parsed request params into the positional list the middlewares work
/// on: absent or null is empty, an array is taken as is, and an object
/// (by-name params) is converted using the configured params.
fn positional_params(
    method: &str,
    params: &[MethodParam],
    parsed: JsonValue,
) -> Result<Vec<JsonValue>, ErrorObjectOwned> {
    match parsed {
        JsonValue::Null => Ok(vec![]),
        JsonValue::Array(list) => Ok(list),
        JsonValue::Object(named) => by_name::named_to_positional(method, params, &named),
        _ => Err(errors::invalid_params("params must be an array or an object")),
    }
}

pub struct SubwayServerHandle {
    pub handle: ServerHandle,
    pub addr: SocketAddr,
    pub extensions: TypeRegistryRef,
}

pub async fn build(config: Config) -> anyhow::Result<SubwayServerHandle> {
    // create extensions registry from config
    let extensions_registry = config
        .extensions
        .create_registry()
        .await
        .expect("Failed to create extensions registry");

    // get the server extension
    let server_builder = extensions_registry
        .read()
        .await
        .get::<SubwayServerBuilder>()
        .expect("Server extension not found");

    let rate_limit_builder = extensions_registry.read().await.get::<RateLimitBuilder>();

    let rpc_method_weights = MethodWeights::from_config(&config.rpcs.methods);

    let request_timeout_seconds = server_builder.config.request_timeout_seconds;

    let metrics = get_rpc_metrics(&extensions_registry).await;

    let registry = extensions_registry.clone();
    let (addr, handle) = server_builder
        .build(rate_limit_builder, rpc_method_weights, metrics, move || async move {
            let mut module = RpcModule::new(());

            let tracer = telemetry::Tracer::new("server");

            // register methods from config
            for method in config.rpcs.methods {
                let mut method_middlewares: Vec<Arc<_>> = vec![];

                for middleware_name in &config.middlewares.methods {
                    if let Some(middleware) =
                        factory::create_method_middleware(middleware_name, &method, &registry).await
                    {
                        method_middlewares.push(middleware.into());
                    }
                }

                let method_middlewares = Middlewares::new(
                    method_middlewares,
                    Arc::new(|_, _| async { Err(errors::failed("Bad configuration")) }.boxed()),
                );

                let method_name = string_to_static_str(method.method.clone());

                let method_params = Arc::new(method.params.clone());

                module.register_async_method(method_name, move |params, _, _| {
                    let method_middlewares = method_middlewares.clone();
                    let method_params = method_params.clone();
                    async move {
                        let params = positional_params(method_name, &method_params, params.parse::<JsonValue>()?)?;

                        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
                        let timeout = tokio::time::Duration::from_secs(request_timeout_seconds);

                        method_middlewares
                            .call(CallRequest::new(method_name, params), result_tx, timeout)
                            .await;

                        let result = result_rx
                            .await
                            .map_err(|_| errors::map_error(jsonrpsee::core::client::Error::RequestTimeout));

                        match result.as_ref() {
                            Ok(Ok(_)) => tracer.span_ok(),
                            Ok(Err(err)) => {
                                if err.code() == INTERNAL_ERROR_CODE {
                                    tracer.span_error(err)
                                } else {
                                    tracer.span_ok()
                                }
                            }
                            Err(err) => {
                                tracer.span_error(err);
                            }
                        };

                        result?
                    }
                    .with_context(tracer.context(method_name))
                })?;
            }

            // register subscriptions from config
            for subscription in config.rpcs.subscriptions {
                let subscribe_name = string_to_static_str(subscription.subscribe.clone());
                let unsubscribe_name = string_to_static_str(subscription.unsubscribe.clone());
                let name = string_to_static_str(subscription.name.clone());
                let subscribe_params = Arc::new(subscription.params.clone());

                let mut subscription_middlewares: Vec<Arc<_>> = vec![];

                for middleware_name in &config.middlewares.subscriptions {
                    if let Some(middleware) =
                        factory::create_subscription_middleware(middleware_name, &subscription, &registry).await
                    {
                        subscription_middlewares.push(middleware.into());
                    }
                }

                let subscription_middlewares = Middlewares::new(
                    subscription_middlewares,
                    Arc::new(|_, _| async { Err("Bad configuration".into()) }.boxed()),
                );

                module.register_subscription(
                    subscribe_name,
                    name,
                    unsubscribe_name,
                    move |params, pending_sink, _, _| {
                        let subscription_middlewares = subscription_middlewares.clone();
                        let subscribe_params = subscribe_params.clone();
                        async move {
                            // A `?` here would drop `pending_sink`, which jsonrpsee answers
                            // with -32603 "Internal error". Reject it instead, so the caller
                            // gets -32602 and the reason, as for a method.
                            let params = match params
                                .parse::<JsonValue>()
                                .and_then(|parsed| positional_params(subscribe_name, &subscribe_params, parsed))
                            {
                                Ok(params) => params,
                                Err(err) => {
                                    tracer.span_ok();
                                    pending_sink.reject(err).await;
                                    return Ok(());
                                }
                            };

                            let (result_tx, result_rx) = tokio::sync::oneshot::channel();
                            let timeout = tokio::time::Duration::from_secs(request_timeout_seconds);

                            subscription_middlewares
                                .call(
                                    SubscriptionRequest {
                                        subscribe: subscribe_name.into(),
                                        params,
                                        unsubscribe: unsubscribe_name.into(),
                                        pending_sink,
                                    },
                                    result_tx,
                                    timeout,
                                )
                                .await;

                            let result = result_rx
                                .await
                                .map_err(|_| errors::map_error(jsonrpsee::core::client::Error::RequestTimeout))?;

                            match result.as_ref() {
                                Ok(_) => {
                                    tracer.span_ok();
                                }
                                Err(err) => {
                                    tracer.span_error(&errors::failed(format!("{err:?}")));
                                }
                            };

                            result
                        }
                        .with_context(tracer.context(name))
                    },
                )?;
            }

            // register aliases from config
            for (alias_old, alias_new) in config.rpcs.aliases {
                let alias_old = string_to_static_str(alias_old);
                let alias_new = string_to_static_str(alias_new);
                module.register_alias(alias_new, alias_old)?;
            }

            let mut rpc_methods = module.method_names().map(|x| x.to_owned()).collect::<Vec<_>>();

            rpc_methods.sort();

            module.register_method("rpc_methods", move |_, _, _| {
                Ok::<JsonValue, ErrorObjectOwned>(json!({
                    "version": 1,
                    "methods": rpc_methods
                }))
            })?;

            Ok(module)
        })
        .await?;

    Ok(SubwayServerHandle {
        addr,
        handle,
        extensions: extensions_registry,
    })
}

#[cfg(test)]
mod tests {
    use jsonrpsee::{
        core::{
            client::{ClientT, SubscriptionClientT},
            params::BatchRequestBuilder,
            traits::ToRpcParams,
        },
        rpc_params,
        server::{ServerBuilder, ServerHandle},
        ws_client::{WsClient, WsClientBuilder},
        RpcModule, SubscriptionMessage,
    };

    use super::*;
    use crate::{
        config::{MiddlewaresConfig, RpcDefinitions, RpcMethod, RpcSubscription},
        extensions::{client::ClientConfig, server::ServerConfig, ExtensionsConfig},
    };

    const TIMEOUT: &str = "call_timeout";
    const CRAZY: &str = "go_crazy";
    const PHO: &str = "call_pho";
    const BAR: &str = "bar";
    const SUB: &str = "subscribe_echo";
    const UNSUB: &str = "unsubscribe_echo";
    const SUB_NAME: &str = "echo";

    async fn subway_server(
        endpoint: String,
        port: u16,
        request_timeout_seconds: Option<u64>,
        max_batch_size: Option<u32>,
    ) -> SubwayServerHandle {
        subway_server_with_subscriptions(endpoint, port, request_timeout_seconds, max_batch_size, vec![]).await
    }

    async fn subway_server_with_subscriptions(
        endpoint: String,
        port: u16,
        request_timeout_seconds: Option<u64>,
        max_batch_size: Option<u32>,
        subscriptions: Vec<RpcSubscription>,
    ) -> SubwayServerHandle {
        let config = Config {
            extensions: ExtensionsConfig {
                client: Some(ClientConfig {
                    endpoints: vec![endpoint],
                    shuffle_endpoints: false,
                    request_timeout_seconds: None,
                    retries: None,
                    failback_interval_seconds: None,
                    unready_rotate_seconds: None,
                }),
                server: Some(ServerConfig {
                    max_response_body_size: None,
                    listen_address: "127.0.0.1".to_string(),
                    port,
                    max_connections: 1024,
                    max_subscriptions_per_connection: 1024,
                    max_batch_size,
                    request_timeout_seconds: request_timeout_seconds.unwrap_or(10),
                    http_methods: Vec::new(),
                    cors: None,
                }),
                ..Default::default()
            },
            middlewares: MiddlewaresConfig {
                methods: vec!["crazy".to_string(), "upstream".to_string()],
                subscriptions: vec!["upstream".to_string()],
            },
            rpcs: RpcDefinitions {
                methods: vec![
                    RpcMethod {
                        method: PHO.to_string(),
                        params: vec![],
                        cache: None,
                        response: None,
                        delay_ms: None,
                        rate_limit_weight: 1,
                    },
                    RpcMethod {
                        method: TIMEOUT.to_string(),
                        params: vec![],
                        cache: None,
                        response: None,
                        delay_ms: None,
                        rate_limit_weight: 1,
                    },
                    RpcMethod {
                        method: CRAZY.to_string(),
                        params: vec![],
                        cache: None,
                        response: None,
                        delay_ms: None,
                        rate_limit_weight: 1,
                    },
                ],
                subscriptions,
                aliases: vec![],
            },
        };
        build(config).await.unwrap()
    }

    async fn upstream_dummy_server(url: &str) -> (String, ServerHandle) {
        let server = ServerBuilder::default()
            .max_request_body_size(u32::MAX)
            .max_response_body_size(u32::MAX)
            .max_connections(10 * 1024)
            .build(url)
            .await
            .unwrap();

        let mut module = RpcModule::new(());
        module
            .register_method(PHO, |_, _, _| Ok::<String, ErrorObjectOwned>(BAR.to_string()))
            .unwrap();
        module
            .register_async_method::<(), _, _>(TIMEOUT, |_, _, _| async {
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            })
            .unwrap();
        // answers one notification: the params it was subscribed with
        module
            .register_subscription(SUB, SUB_NAME, UNSUB, |params, pending, _, _| async move {
                let params = params.parse::<JsonValue>()?;
                let sink = pending.accept().await?;
                sink.send(SubscriptionMessage::from_json(&params)?).await?;
                futures::future::pending::<()>().await;
                Ok(())
            })
            .unwrap();
        let addr = format!("ws://{}", server.local_addr().unwrap());
        let handle = server.start(module);
        (addr, handle)
    }

    fn subscription() -> RpcSubscription {
        let param = |name: &str, optional: bool, aliases: &[&str]| MethodParam {
            name: name.to_string(),
            ty: String::new(),
            optional,
            inject: false,
            aliases: aliases.iter().map(|a| a.to_string()).collect(),
        };
        RpcSubscription {
            subscribe: SUB.to_string(),
            unsubscribe: UNSUB.to_string(),
            name: SUB_NAME.to_string(),
            merge_strategy: None,
            params: vec![param("withRuntime", true, &[]), param("keys", true, &["list"])],
        }
    }

    /// Params sent exactly as written, for shapes `rpc_params!` cannot produce.
    struct RawParams(&'static str);

    impl ToRpcParams for RawParams {
        fn to_rpc_params(self) -> Result<Option<Box<serde_json::value::RawValue>>, serde_json::Error> {
            serde_json::value::RawValue::from_string(self.0.to_string()).map(Some)
        }
    }

    async fn subscribe_error(client: &WsClient, params: &'static str) -> ErrorObjectOwned {
        match client
            .subscribe::<JsonValue, _>(SUB, RawParams(params), UNSUB)
            .await
            .map(|_| ())
        {
            Err(jsonrpsee::core::client::Error::Call(err)) => err,
            other => panic!("{params}: expected a call error, got {other:?}"),
        }
    }

    async fn subscribe_first(client: &WsClient, params: &'static str) -> JsonValue {
        let mut sub = client
            .subscribe::<JsonValue, _>(SUB, RawParams(params), UNSUB)
            .await
            .unwrap_or_else(|e| panic!("{params}: {e:?}"));
        tokio::time::timeout(std::time::Duration::from_secs(5), sub.next())
            .await
            .unwrap_or_else(|_| panic!("{params}: no notification"))
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn subscribe_with_bad_params_is_invalid_params_not_internal_error() {
        let (endpoint, upstream_dummy_server_handle) = upstream_dummy_server("127.0.0.1:9960").await;
        let subway_server = subway_server_with_subscriptions(endpoint, 9949, None, None, vec![subscription()]).await;
        let url = format!("ws://{}", subway_server.addr);
        let client = ws_client(&url).await;

        // unknown by-name key
        let err = subscribe_error(&client, r#"{"x":1}"#).await;
        assert_eq!(err.code(), -32602, "{err:?}");
        let data = err.data().unwrap().get();
        assert!(data.contains("unknown param \\\"x\\\""), "{data}");
        assert!(data.contains("withRuntime"), "{data}");

        // two keys for one param
        let err = subscribe_error(&client, r#"{"with_runtime":false,"withRuntime":true}"#).await;
        assert_eq!(err.code(), -32602, "{err:?}");
        assert!(err.data().unwrap().get().contains("both name param"), "{err:?}");

        // params that are neither an array nor an object
        for params in ["1", "\"a\"", "true"] {
            let err = subscribe_error(&client, params).await;
            assert_eq!(err.code(), -32602, "{params}: {err:?}");
            assert!(err.data().unwrap().get().contains("array or an object"), "{err:?}");
        }

        // the connection is still usable afterwards
        assert_eq!(subscribe_first(&client, "[]").await, json!([]));

        subway_server.handle.stop().unwrap();
        upstream_dummy_server_handle.stop().unwrap();
    }

    #[tokio::test]
    async fn valid_subscribes_reach_upstream_unchanged() {
        let (endpoint, upstream_dummy_server_handle) = upstream_dummy_server("127.0.0.1:9961").await;
        let subway_server = subway_server_with_subscriptions(endpoint, 9950, None, None, vec![subscription()]).await;
        let url = format!("ws://{}", subway_server.addr);
        let client = ws_client(&url).await;

        // positional params are forwarded as given, whatever the configured params say
        assert_eq!(subscribe_first(&client, "[]").await, json!([]));
        assert_eq!(subscribe_first(&client, "[true]").await, json!([true]));
        assert_eq!(
            subscribe_first(&client, r#"[null,["0x01"],"extra"]"#).await,
            json!([null, ["0x01"], "extra"])
        );
        // null params are an empty list, as before
        assert_eq!(subscribe_first(&client, "null").await, json!([]));

        // by-name params become positional ones
        assert_eq!(subscribe_first(&client, "{}").await, json!([]));
        assert_eq!(
            subscribe_first(&client, r#"{"with_runtime":true}"#).await,
            json!([true])
        );
        assert_eq!(
            subscribe_first(&client, r#"{"list":["0x01"]}"#).await,
            json!([null, ["0x01"]])
        );

        subway_server.handle.stop().unwrap();
        upstream_dummy_server_handle.stop().unwrap();
    }

    async fn ws_client(url: &str) -> WsClient {
        WsClientBuilder::default()
            .request_timeout(std::time::Duration::from_secs(60))
            .max_request_size(u32::MAX)
            .max_concurrent_requests(1024 * 1024)
            .build(url)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn null_param_works() {
        let (endpoint, upstream_dummy_server_handle) = upstream_dummy_server("127.0.0.1:9955").await;
        let subway_server = subway_server(endpoint, 9944, None, None).await;
        let url = format!("ws://{}", subway_server.addr);
        let client = ws_client(&url).await;
        assert_eq!(BAR, client.request::<String, _>(PHO, rpc_params!()).await.unwrap());
        subway_server.handle.stop().unwrap();
        upstream_dummy_server_handle.stop().unwrap();
    }

    #[tokio::test]
    async fn request_timeout() {
        let (endpoint, upstream_dummy_server_handle) = upstream_dummy_server("127.0.0.1:9956").await;
        // server with 1 second timeout
        let subway_server = subway_server(endpoint, 9945, Some(1), None).await;
        let url = format!("ws://{}", subway_server.addr);
        // client with default 60 second timeout
        let client = ws_client(&url).await;

        // timeout when middleware goes crazy
        {
            let now = std::time::Instant::now();
            let err = client.request::<String, _>(CRAZY, rpc_params!()).await.unwrap_err();
            // should timeout in 1 second
            assert_eq!(now.elapsed().as_secs(), 1);
            assert!(err.to_string().contains("Request timeout"));
        }

        // timeout when request takes too long
        {
            let now = std::time::Instant::now();
            let err = client.request::<String, _>(TIMEOUT, rpc_params!()).await.unwrap_err();
            // should timeout in 1 second
            assert_eq!(now.elapsed().as_secs(), 1);
            assert!(err.to_string().contains("Request timeout"));
        }

        subway_server.handle.stop().unwrap();
        upstream_dummy_server_handle.stop().unwrap();
    }

    #[tokio::test]
    async fn batch_requests_works() {
        let (endpoint, upstream_dummy_server_handle) = upstream_dummy_server("127.0.0.1:9957").await;

        // Server with max batch size 3
        let subway_server = subway_server(endpoint, 9946, None, Some(3)).await;
        let url = format!("ws://{}", subway_server.addr);
        let client = ws_client(&url).await;

        // Sending 3 request in a batch
        let mut batch = BatchRequestBuilder::new();
        batch.insert(PHO, rpc_params!()).unwrap();
        batch.insert(PHO, rpc_params!()).unwrap();
        batch.insert(PHO, rpc_params!()).unwrap();

        let res = client.batch_request::<String>(batch).await.unwrap();
        assert_eq!(res.num_successful_calls(), 3);

        upstream_dummy_server_handle.stop().unwrap();
    }

    #[tokio::test]
    async fn batch_requests_exceeds_max_size_errors() {
        let (endpoint, upstream_dummy_server_handle) = upstream_dummy_server("127.0.0.1:9958").await;

        // Server with max batch size 3
        let subway_server = subway_server(endpoint, 9947, None, Some(3)).await;
        let url = format!("ws://{}", subway_server.addr);
        let client = ws_client(&url).await;

        // Sending 4 request in a batch
        let mut batch = BatchRequestBuilder::new();
        batch.insert(PHO, rpc_params!()).unwrap();
        batch.insert(PHO, rpc_params!()).unwrap();
        batch.insert(PHO, rpc_params!()).unwrap();
        batch.insert(PHO, rpc_params!()).unwrap();

        // Due to the limitation of jsonrpsee client implementation,
        // we can't check the error message when response batch id is `null`.
        // E.g.
        // Raw response - `{"jsonrpc":"2.0","error":{"code":-32010,"message":"The batch request was too large","data":"Exceeded max limit of 3"},"id":null}`
        // Jsonrpsee client response - `Err(RestartNeeded(InvalidRequestId(NotPendingRequest("null"))))`
        //
        // Checking if error is returned for now.
        let res = client.batch_request::<String>(batch).await;

        assert!(res.is_err());

        upstream_dummy_server_handle.stop().unwrap();
    }

    #[tokio::test]
    async fn batch_requests_disabled_errors() {
        let (endpoint, upstream_dummy_server_handle) = upstream_dummy_server("127.0.0.1:9959").await;

        // Server with max batch size 0 (disabled)
        let subway_server = subway_server(endpoint, 9948, None, Some(0)).await;
        let url = format!("ws://{}", subway_server.addr);
        let client = ws_client(&url).await;

        // Sending 1 request in a batch
        let mut batch = BatchRequestBuilder::new();
        batch.insert(PHO, rpc_params!()).unwrap();

        // Due to the limitation of jsonrpsee client implementation,
        // we can't check the error message when response batch id is `null`.
        // E.g.
        // Raw response - `{"jsonrpc":"2.0","error":{"code":-32005,"message":"Batched requests are not supported by this server"},"id":null}`
        // Jsonrpsee client response - `Err(RestartNeeded(InvalidRequestId(NotPendingRequest("null"))))`
        //
        // Checking if error is returned for now.
        let res = client.batch_request::<String>(batch).await;

        assert!(res.is_err());

        upstream_dummy_server_handle.stop().unwrap();
    }
}
