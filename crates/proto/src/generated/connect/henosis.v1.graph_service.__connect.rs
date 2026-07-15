///Shorthand for `OwnedView<CreateGraphRequestView<'static>>`.
pub type OwnedCreateGraphRequestView = ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::CreateGraphRequestView<'static>,
>;
///Shorthand for `OwnedView<CreateGraphResponseView<'static>>`.
pub type OwnedCreateGraphResponseView = ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::CreateGraphResponseView<'static>,
>;
///Shorthand for `OwnedView<UpdateGraphRequestView<'static>>`.
pub type OwnedUpdateGraphRequestView = ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::UpdateGraphRequestView<'static>,
>;
///Shorthand for `OwnedView<UpdateGraphResponseView<'static>>`.
pub type OwnedUpdateGraphResponseView = ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::UpdateGraphResponseView<'static>,
>;
///Shorthand for `OwnedView<RetireGraphRequestView<'static>>`.
pub type OwnedRetireGraphRequestView = ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::RetireGraphRequestView<'static>,
>;
///Shorthand for `OwnedView<RetireGraphResponseView<'static>>`.
pub type OwnedRetireGraphResponseView = ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::RetireGraphResponseView<'static>,
>;
///Shorthand for `OwnedView<GetGraphRequestView<'static>>`.
pub type OwnedGetGraphRequestView = ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::GetGraphRequestView<'static>,
>;
///Shorthand for `OwnedView<GetGraphResponseView<'static>>`.
pub type OwnedGetGraphResponseView = ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::GetGraphResponseView<'static>,
>;
///Shorthand for `OwnedView<WatchGraphRequestView<'static>>`.
pub type OwnedWatchGraphRequestView = ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::WatchGraphRequestView<'static>,
>;
///Shorthand for `OwnedView<WatchGraphResponseView<'static>>`.
pub type OwnedWatchGraphResponseView = ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::WatchGraphResponseView<'static>,
>;
///Shorthand for `OwnedView<PullSlicesRequestView<'static>>`.
pub type OwnedPullSlicesRequestView = ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::PullSlicesRequestView<'static>,
>;
///Shorthand for `OwnedView<PullSlicesResponseView<'static>>`.
pub type OwnedPullSlicesResponseView = ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::PullSlicesResponseView<'static>,
>;
///Shorthand for `OwnedView<ReportSliceRequestView<'static>>`.
pub type OwnedReportSliceRequestView = ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::ReportSliceRequestView<'static>,
>;
///Shorthand for `OwnedView<ReportSliceResponseView<'static>>`.
pub type OwnedReportSliceResponseView = ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::ReportSliceResponseView<'static>,
>;
impl ::connectrpc::Encodable<crate::proto::henosis::v1::CreateGraphResponse>
for crate::proto::henosis::v1::__buffa::view::CreateGraphResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::henosis::v1::CreateGraphResponse>
for ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::CreateGraphResponseView<'static>,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::henosis::v1::UpdateGraphResponse>
for crate::proto::henosis::v1::__buffa::view::UpdateGraphResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::henosis::v1::UpdateGraphResponse>
for ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::UpdateGraphResponseView<'static>,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::henosis::v1::RetireGraphResponse>
for crate::proto::henosis::v1::__buffa::view::RetireGraphResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::henosis::v1::RetireGraphResponse>
for ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::RetireGraphResponseView<'static>,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::henosis::v1::GetGraphResponse>
for crate::proto::henosis::v1::__buffa::view::GetGraphResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::henosis::v1::GetGraphResponse>
for ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::GetGraphResponseView<'static>,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::henosis::v1::WatchGraphResponse>
for crate::proto::henosis::v1::__buffa::view::WatchGraphResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::henosis::v1::WatchGraphResponse>
for ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::WatchGraphResponseView<'static>,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::henosis::v1::PullSlicesResponse>
for crate::proto::henosis::v1::__buffa::view::PullSlicesResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::henosis::v1::PullSlicesResponse>
for ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::PullSlicesResponseView<'static>,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::henosis::v1::ReportSliceResponse>
for crate::proto::henosis::v1::__buffa::view::ReportSliceResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::henosis::v1::ReportSliceResponse>
for ::buffa::view::OwnedView<
    crate::proto::henosis::v1::__buffa::view::ReportSliceResponseView<'static>,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
}
/// Full service name for this service.
pub const GRAPH_SERVICE_SERVICE_NAME: &str = "henosis.v1.GraphService";
/// Static [`Spec`](::connectrpc::Spec) for the server-side `CreateGraph` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const GRAPH_SERVICE_CREATE_GRAPH_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/henosis.v1.GraphService/CreateGraph",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Idempotent);
/// Static [`Spec`](::connectrpc::Spec) for the server-side `UpdateGraph` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const GRAPH_SERVICE_UPDATE_GRAPH_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/henosis.v1.GraphService/UpdateGraph",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Idempotent);
/// Static [`Spec`](::connectrpc::Spec) for the server-side `RetireGraph` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const GRAPH_SERVICE_RETIRE_GRAPH_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/henosis.v1.GraphService/RetireGraph",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Idempotent);
/// Static [`Spec`](::connectrpc::Spec) for the server-side `GetGraph` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const GRAPH_SERVICE_GET_GRAPH_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/henosis.v1.GraphService/GetGraph",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::NoSideEffects);
/// Static [`Spec`](::connectrpc::Spec) for the server-side `WatchGraph` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const GRAPH_SERVICE_WATCH_GRAPH_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/henosis.v1.GraphService/WatchGraph",
        ::connectrpc::StreamType::ServerStream,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::NoSideEffects);
/// Server trait for GraphService.
///
/// # Implementing handlers
///
/// Implement methods with plain `async fn`; the returned future satisfies
/// the `Send` bound automatically.
///
/// **Unary and server-streaming requests** arrive as
/// [`ServiceRequest<'_, Req>`](::connectrpc::ServiceRequest): a zero-copy
/// view of the request plus its body, valid for the duration of the call.
/// Fields are read directly (`request.name` is a `&str` into the decoded
/// buffer) and the borrow may be held across `.await` points. Anything
/// that must outlive the call — `tokio::spawn`, channels, server state,
/// or data captured by a returned response stream — takes owned data:
/// call `request.to_owned_message()` (or copy the specific fields)
/// first.
///
/// **Client-streaming and bidi requests** arrive as
/// [`InboundStream<Req>`](::connectrpc::InboundStream) — a
/// `ServiceStream` of [`StreamMessage`](::connectrpc::StreamMessage)s.
/// Each item owns its decoded buffer and is `Send + 'static`, so items
/// can be buffered or moved into spawned tasks; read fields zero-copy
/// through the generated accessor methods (`item.name()`) or `.view()`,
/// convert with `.to_owned_message()`, or yield an item back unchanged —
/// `StreamMessage<M>` implements `Encodable<M>`.
///
/// Request types resolved through `extern_path` (e.g. well-known types
/// from another crate) use the same wrappers; the crate that owns the
/// type must be generated with buffa ≥ 0.8.0 and views enabled so the
/// backing `HasMessageView` impl exists.
///
/// The `impl Encodable<Out>` return bound accepts the owned `Out`, the
/// generated `OutView<'_>` / `OwnedOutView`,
/// [`MaybeBorrowed`](::connectrpc::MaybeBorrowed), or
/// [`PreEncoded`](::connectrpc::PreEncoded) for handlers that encode a
/// non-`'static` view internally and pass the bytes across the handler
/// boundary. View bodies are not emitted for output types mapped via
/// `extern_path` (the impl would be an orphan); return owned for
/// WKT/extern outputs.
///
/// Server-streaming and bidi-streaming methods return
/// `ServiceStream<impl Encodable<Out> + Send + use<Self>>`. The
/// `use<Self>` precise-capturing clause excludes `&self`'s lifetime and
/// the request's lifetime (unary methods use `use<'a, Self>` and may
/// borrow from `&self`), so stream items must be `'static` and cannot
/// borrow from the request. To stream view-encoded data, encode each
/// item inside the stream body and yield
/// [`PreEncoded`](::connectrpc::PreEncoded) — see its `# Streaming
/// example` doc.
#[allow(clippy::type_complexity)]
pub trait GraphService: Send + Sync + 'static {
    /// Handle the CreateGraph RPC.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call; message fields are read directly on it
    /// (zero-copy). The response cannot borrow from `request` — use
    /// `.to_owned_message()` (or copy the specific fields) for anything
    /// returned, stored, or moved into `tokio::spawn`.
    fn create_graph<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<
            '_,
            crate::proto::henosis::v1::CreateGraphRequest,
        >,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::henosis::v1::CreateGraphResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
    /// Handle the UpdateGraph RPC.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call; message fields are read directly on it
    /// (zero-copy). The response cannot borrow from `request` — use
    /// `.to_owned_message()` (or copy the specific fields) for anything
    /// returned, stored, or moved into `tokio::spawn`.
    fn update_graph<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<
            '_,
            crate::proto::henosis::v1::UpdateGraphRequest,
        >,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::henosis::v1::UpdateGraphResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
    /// Handle the RetireGraph RPC.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call; message fields are read directly on it
    /// (zero-copy). The response cannot borrow from `request` — use
    /// `.to_owned_message()` (or copy the specific fields) for anything
    /// returned, stored, or moved into `tokio::spawn`.
    fn retire_graph<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<
            '_,
            crate::proto::henosis::v1::RetireGraphRequest,
        >,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::henosis::v1::RetireGraphResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
    /// Handle the GetGraph RPC.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call; message fields are read directly on it
    /// (zero-copy). The response cannot borrow from `request` — use
    /// `.to_owned_message()` (or copy the specific fields) for anything
    /// returned, stored, or moved into `tokio::spawn`.
    fn get_graph<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<
            '_,
            crate::proto::henosis::v1::GetGraphRequest,
        >,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::henosis::v1::GetGraphResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
    /// Handle the WatchGraph RPC.
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call (until the response stream is returned);
    /// message fields are read directly on it (zero-copy). Data the
    /// returned stream needs must be copied out or converted via
    /// `.to_owned_message()`.
    fn watch_graph(
        &self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<
            '_,
            crate::proto::henosis::v1::WatchGraphRequest,
        >,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            ::connectrpc::ServiceStream<
                impl ::connectrpc::Encodable<
                    crate::proto::henosis::v1::WatchGraphResponse,
                > + Send + use<Self>,
            >,
        >,
    > + Send;
}
/// Extension trait for registering a service implementation with a Router.
///
/// This trait is automatically implemented for all types that implement the service trait.
/// Prefer [`Router::add_service`](::connectrpc::Router::add_service) for
/// top-down registration; `register` remains available for compatibility
/// and cases where the service-first call shape is more convenient.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
///
/// let service = Arc::new(MyServiceImpl);
/// let router = service.register(Router::new());
/// ```
pub trait GraphServiceExt: GraphService {
    /// Register this service implementation with a Router.
    ///
    /// Takes ownership of the `Arc<Self>` and returns a new Router with
    /// this service's methods registered.
    fn register(
        self: ::std::sync::Arc<Self>,
        router: ::connectrpc::Router,
    ) -> ::connectrpc::Router;
}
impl<S: GraphService> GraphServiceExt for S {
    fn register(
        self: ::std::sync::Arc<Self>,
        router: ::connectrpc::Router,
    ) -> ::connectrpc::Router {
        router
            .route_view(
                GRAPH_SERVICE_SERVICE_NAME,
                "CreateGraph",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            crate::proto::henosis::v1::__buffa::view::CreateGraphRequestView<
                                'static,
                            >,
                        >,
                        format|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                crate::proto::henosis::v1::CreateGraphRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.create_graph(ctx, sreq)
                                .await?
                                .encode::<
                                    crate::proto::henosis::v1::CreateGraphResponse,
                                >(format)
                        }
                    })
                },
            )
            .with_spec(GRAPH_SERVICE_CREATE_GRAPH_SPEC)
            .route_view(
                GRAPH_SERVICE_SERVICE_NAME,
                "UpdateGraph",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            crate::proto::henosis::v1::__buffa::view::UpdateGraphRequestView<
                                'static,
                            >,
                        >,
                        format|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                crate::proto::henosis::v1::UpdateGraphRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.update_graph(ctx, sreq)
                                .await?
                                .encode::<
                                    crate::proto::henosis::v1::UpdateGraphResponse,
                                >(format)
                        }
                    })
                },
            )
            .with_spec(GRAPH_SERVICE_UPDATE_GRAPH_SPEC)
            .route_view(
                GRAPH_SERVICE_SERVICE_NAME,
                "RetireGraph",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            crate::proto::henosis::v1::__buffa::view::RetireGraphRequestView<
                                'static,
                            >,
                        >,
                        format|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                crate::proto::henosis::v1::RetireGraphRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.retire_graph(ctx, sreq)
                                .await?
                                .encode::<
                                    crate::proto::henosis::v1::RetireGraphResponse,
                                >(format)
                        }
                    })
                },
            )
            .with_spec(GRAPH_SERVICE_RETIRE_GRAPH_SPEC)
            .route_view_idempotent(
                GRAPH_SERVICE_SERVICE_NAME,
                "GetGraph",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            crate::proto::henosis::v1::__buffa::view::GetGraphRequestView<
                                'static,
                            >,
                        >,
                        format|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                crate::proto::henosis::v1::GetGraphRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.get_graph(ctx, sreq)
                                .await?
                                .encode::<
                                    crate::proto::henosis::v1::GetGraphResponse,
                                >(format)
                        }
                    })
                },
            )
            .with_spec(GRAPH_SERVICE_GET_GRAPH_SPEC)
            .route_view_server_stream::<
                _,
                _,
                crate::proto::henosis::v1::WatchGraphResponse,
            >(
                GRAPH_SERVICE_SERVICE_NAME,
                "WatchGraph",
                ::connectrpc::view_streaming_handler_fn({
                    let svc = ::std::sync::Arc::clone(&self);
                    move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            crate::proto::henosis::v1::__buffa::view::WatchGraphRequestView<
                                'static,
                            >,
                        >|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                crate::proto::henosis::v1::WatchGraphRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.watch_graph(ctx, sreq).await
                        }
                    }
                }),
            )
            .with_spec(GRAPH_SERVICE_WATCH_GRAPH_SPEC)
    }
}
/// Type-inference marker used by [`Router::add_service`](::connectrpc::Router::add_service).
#[doc(hidden)]
pub struct GraphServiceRegisterMarker;
impl<S: GraphService> ::connectrpc::ServiceRegister<GraphServiceRegisterMarker>
for ::std::sync::Arc<S> {
    fn register_service(self, router: ::connectrpc::Router) -> ::connectrpc::Router {
        <S as GraphServiceExt>::register(self, router)
    }
}
/// Monomorphic dispatcher for `GraphService`.
///
/// Unlike `.register(Router)` which type-erases each method into an `Arc<dyn ErasedHandler>` stored in a `HashMap`, this struct dispatches via a compile-time `match` on method name: no vtable, no hash lookup.
///
/// # Example
///
/// ```rust,ignore
/// use connectrpc::ConnectRpcService;
///
/// let server = GraphServiceServer::new(MyImpl);
/// let service = ConnectRpcService::new(server);
/// // hand `service` to axum/hyper as a fallback_service
/// ```
pub struct GraphServiceServer<T> {
    inner: ::std::sync::Arc<T>,
}
impl<T: GraphService> GraphServiceServer<T> {
    /// Wrap a service implementation in a monomorphic dispatcher.
    pub fn new(service: T) -> Self {
        Self {
            inner: ::std::sync::Arc::new(service),
        }
    }
    /// Wrap an already-`Arc`'d service implementation.
    pub fn from_arc(inner: ::std::sync::Arc<T>) -> Self {
        Self { inner }
    }
}
impl<T> Clone for GraphServiceServer<T> {
    fn clone(&self) -> Self {
        Self {
            inner: ::std::sync::Arc::clone(&self.inner),
        }
    }
}
impl<T: GraphService> ::connectrpc::Dispatcher for GraphServiceServer<T> {
    #[inline]
    fn lookup(
        &self,
        path: &str,
    ) -> Option<::connectrpc::dispatcher::codegen::MethodDescriptor> {
        let method = path.strip_prefix("henosis.v1.GraphService/")?;
        match method {
            "CreateGraph" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(false)
                        .with_spec(GRAPH_SERVICE_CREATE_GRAPH_SPEC),
                )
            }
            "UpdateGraph" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(false)
                        .with_spec(GRAPH_SERVICE_UPDATE_GRAPH_SPEC),
                )
            }
            "RetireGraph" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(false)
                        .with_spec(GRAPH_SERVICE_RETIRE_GRAPH_SPEC),
                )
            }
            "GetGraph" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(true)
                        .with_spec(GRAPH_SERVICE_GET_GRAPH_SPEC),
                )
            }
            "WatchGraph" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::server_streaming()
                        .with_spec(GRAPH_SERVICE_WATCH_GRAPH_SPEC),
                )
            }
            _ => None,
        }
    }
    fn call_unary(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::Payload,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::UnaryResult {
        let Some(method) = path.strip_prefix("henosis.v1.GraphService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_unary(path);
        };
        let _ = (&ctx, &request, &format);
        match method {
            "CreateGraph" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        crate::proto::henosis::v1::CreateGraphRequest,
                    >(request.encoded()?, format)?;
                    let req: crate::proto::henosis::v1::__buffa::view::CreateGraphRequestView<
                        '_,
                    > = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        crate::proto::henosis::v1::CreateGraphRequest,
                    >::from_parts(&req, &body);
                    svc.create_graph(ctx, req)
                        .await?
                        .encode::<crate::proto::henosis::v1::CreateGraphResponse>(format)
                })
            }
            "UpdateGraph" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        crate::proto::henosis::v1::UpdateGraphRequest,
                    >(request.encoded()?, format)?;
                    let req: crate::proto::henosis::v1::__buffa::view::UpdateGraphRequestView<
                        '_,
                    > = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        crate::proto::henosis::v1::UpdateGraphRequest,
                    >::from_parts(&req, &body);
                    svc.update_graph(ctx, req)
                        .await?
                        .encode::<crate::proto::henosis::v1::UpdateGraphResponse>(format)
                })
            }
            "RetireGraph" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        crate::proto::henosis::v1::RetireGraphRequest,
                    >(request.encoded()?, format)?;
                    let req: crate::proto::henosis::v1::__buffa::view::RetireGraphRequestView<
                        '_,
                    > = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        crate::proto::henosis::v1::RetireGraphRequest,
                    >::from_parts(&req, &body);
                    svc.retire_graph(ctx, req)
                        .await?
                        .encode::<crate::proto::henosis::v1::RetireGraphResponse>(format)
                })
            }
            "GetGraph" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        crate::proto::henosis::v1::GetGraphRequest,
                    >(request.encoded()?, format)?;
                    let req: crate::proto::henosis::v1::__buffa::view::GetGraphRequestView<
                        '_,
                    > = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        crate::proto::henosis::v1::GetGraphRequest,
                    >::from_parts(&req, &body);
                    svc.get_graph(ctx, req)
                        .await?
                        .encode::<crate::proto::henosis::v1::GetGraphResponse>(format)
                })
            }
            _ => ::connectrpc::dispatcher::codegen::unimplemented_unary(path),
        }
    }
    fn call_server_streaming(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        request: ::buffa::bytes::Bytes,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::StreamingResult {
        let Some(method) = path.strip_prefix("henosis.v1.GraphService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_streaming(path);
        };
        let _ = (&ctx, &request, &format);
        match method {
            "WatchGraph" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        crate::proto::henosis::v1::WatchGraphRequest,
                    >(request, format)?;
                    let req: crate::proto::henosis::v1::__buffa::view::WatchGraphRequestView<
                        '_,
                    > = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        crate::proto::henosis::v1::WatchGraphRequest,
                    >::from_parts(&req, &body);
                    let resp = svc.watch_graph(ctx, req).await?;
                    Ok(
                        resp
                            .map_body(|s| ::connectrpc::dispatcher::codegen::encode_response_stream::<
                                crate::proto::henosis::v1::WatchGraphResponse,
                                _,
                                _,
                            >(s, format)),
                    )
                })
            }
            _ => ::connectrpc::dispatcher::codegen::unimplemented_streaming(path),
        }
    }
    fn call_client_streaming(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        requests: ::connectrpc::dispatcher::codegen::RequestStream,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::UnaryResult {
        let Some(method) = path.strip_prefix("henosis.v1.GraphService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_unary(path);
        };
        let _ = (&ctx, &requests, &format);
        match method {
            _ => ::connectrpc::dispatcher::codegen::unimplemented_unary(path),
        }
    }
    fn call_bidi_streaming(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        requests: ::connectrpc::dispatcher::codegen::RequestStream,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::StreamingResult {
        let Some(method) = path.strip_prefix("henosis.v1.GraphService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_streaming(path);
        };
        let _ = (&ctx, &requests, &format);
        match method {
            _ => ::connectrpc::dispatcher::codegen::unimplemented_streaming(path),
        }
    }
}
/// Client for this service.
///
/// Generic over `T: ClientTransport`. For **gRPC** (HTTP/2), use
/// `Http2Connection` — it has honest `poll_ready` and composes with
/// `tower::balance` for multi-connection load balancing. For **Connect
/// over HTTP/1.1** (or unknown protocol), use `HttpClient`.
///
/// # Example (gRPC / HTTP/2)
///
/// ```rust,ignore
/// use connectrpc::client::{Http2Connection, ClientConfig};
/// use connectrpc::Protocol;
///
/// let uri: http::Uri = "http://localhost:8080".parse()?;
/// let conn = Http2Connection::connect_plaintext(uri.clone()).await?.shared(1024);
/// let config = ClientConfig::new(uri).with_protocol(Protocol::Grpc);
///
/// let client = GraphServiceClient::new(conn, config);
/// let response = client.create_graph(request).await?;
/// ```
///
/// # Example (Connect / HTTP/1.1 or ALPN)
///
/// ```rust,ignore
/// use connectrpc::client::{HttpClient, ClientConfig};
///
/// let http = HttpClient::plaintext();  // cleartext http:// only
/// let config = ClientConfig::new("http://localhost:8080".parse()?);
///
/// let client = GraphServiceClient::new(http, config);
/// let response = client.create_graph(request).await?;
/// ```
///
/// # Working with the response
///
/// Unary calls return [`UnaryResponse<OwnedView<FooView>>`](::connectrpc::client::UnaryResponse).
/// [`view()`](::connectrpc::client::UnaryResponse::view) borrows the response
/// message, so field access is zero-copy:
///
/// ```rust,ignore
/// let resp = client.create_graph(request).await?;
/// let name: &str = resp.view().name;  // borrow into the response buffer
/// ```
///
/// If you need the owned struct (e.g. to store or pass by value), use
/// [`into_owned()`](::connectrpc::client::UnaryResponse::into_owned):
///
/// ```rust,ignore
/// let owned = client.create_graph(request).await?.into_owned();
/// ```
///
/// [`into_view()`](::connectrpc::client::UnaryResponse::into_view) keeps the
/// zero-copy decoded body (an `OwnedView`) without copying; field access on it
/// goes through `.reborrow()`. Streaming responses yield one
/// [`StreamMessage`](::connectrpc::StreamMessage) per received message from
/// `.message().await` — read fields zero-copy through the generated accessor
/// methods (`msg.name()`) or `.view()`, or convert with `.to_owned_message()`.
#[derive(Clone)]
pub struct GraphServiceClient<T> {
    transport: T,
    config: ::connectrpc::client::ClientConfig,
}
impl<T> GraphServiceClient<T>
where
    T: ::connectrpc::client::ClientTransport,
    <T::ResponseBody as ::connectrpc::http_body::Body>::Error: ::std::fmt::Display,
{
    /// Create a new client with the given transport and configuration.
    pub fn new(transport: T, config: ::connectrpc::client::ClientConfig) -> Self {
        Self { transport, config }
    }
    /// Get the client configuration.
    pub fn config(&self) -> &::connectrpc::client::ClientConfig {
        &self.config
    }
    /// Get a mutable reference to the client configuration.
    pub fn config_mut(&mut self) -> &mut ::connectrpc::client::ClientConfig {
        &mut self.config
    }
    /// Call the CreateGraph RPC. Sends a request to /henosis.v1.GraphService/CreateGraph.
    pub async fn create_graph(
        &self,
        request: crate::proto::henosis::v1::CreateGraphRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::henosis::v1::__buffa::view::CreateGraphResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.create_graph_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the CreateGraph RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn create_graph_with_options(
        &self,
        request: crate::proto::henosis::v1::CreateGraphRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::henosis::v1::__buffa::view::CreateGraphResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                GRAPH_SERVICE_SERVICE_NAME,
                "CreateGraph",
                request,
                options,
            )
            .await
    }
    /// Call the UpdateGraph RPC. Sends a request to /henosis.v1.GraphService/UpdateGraph.
    pub async fn update_graph(
        &self,
        request: crate::proto::henosis::v1::UpdateGraphRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::henosis::v1::__buffa::view::UpdateGraphResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.update_graph_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the UpdateGraph RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn update_graph_with_options(
        &self,
        request: crate::proto::henosis::v1::UpdateGraphRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::henosis::v1::__buffa::view::UpdateGraphResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                GRAPH_SERVICE_SERVICE_NAME,
                "UpdateGraph",
                request,
                options,
            )
            .await
    }
    /// Call the RetireGraph RPC. Sends a request to /henosis.v1.GraphService/RetireGraph.
    pub async fn retire_graph(
        &self,
        request: crate::proto::henosis::v1::RetireGraphRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::henosis::v1::__buffa::view::RetireGraphResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.retire_graph_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the RetireGraph RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn retire_graph_with_options(
        &self,
        request: crate::proto::henosis::v1::RetireGraphRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::henosis::v1::__buffa::view::RetireGraphResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                GRAPH_SERVICE_SERVICE_NAME,
                "RetireGraph",
                request,
                options,
            )
            .await
    }
    /// Call the GetGraph RPC. Sends a request to /henosis.v1.GraphService/GetGraph.
    pub async fn get_graph(
        &self,
        request: crate::proto::henosis::v1::GetGraphRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::henosis::v1::__buffa::view::GetGraphResponseView<'static>,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.get_graph_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the GetGraph RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn get_graph_with_options(
        &self,
        request: crate::proto::henosis::v1::GetGraphRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::henosis::v1::__buffa::view::GetGraphResponseView<'static>,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                GRAPH_SERVICE_SERVICE_NAME,
                "GetGraph",
                request,
                options,
            )
            .await
    }
    /// Call the WatchGraph RPC. Sends a request to /henosis.v1.GraphService/WatchGraph.
    pub async fn watch_graph(
        &self,
        request: crate::proto::henosis::v1::WatchGraphRequest,
    ) -> Result<
        ::connectrpc::client::ServerStream<
            T::ResponseBody,
            crate::proto::henosis::v1::__buffa::view::WatchGraphResponseView<'static>,
        >,
        ::connectrpc::ConnectError,
    > {
        self.watch_graph_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the WatchGraph RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn watch_graph_with_options(
        &self,
        request: crate::proto::henosis::v1::WatchGraphRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::ServerStream<
            T::ResponseBody,
            crate::proto::henosis::v1::__buffa::view::WatchGraphResponseView<'static>,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_server_stream(
                &self.transport,
                &self.config,
                GRAPH_SERVICE_SERVICE_NAME,
                "WatchGraph",
                request,
                options,
            )
            .await
    }
}
/// Full service name for this service.
pub const CONTROLLER_SERVICE_SERVICE_NAME: &str = "henosis.v1.ControllerService";
/// Static [`Spec`](::connectrpc::Spec) for the server-side `PullSlices` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const CONTROLLER_SERVICE_PULL_SLICES_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/henosis.v1.ControllerService/PullSlices",
        ::connectrpc::StreamType::ServerStream,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::NoSideEffects);
/// Static [`Spec`](::connectrpc::Spec) for the server-side `ReportSlice` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const CONTROLLER_SERVICE_REPORT_SLICE_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/henosis.v1.ControllerService/ReportSlice",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Idempotent);
/// Controllers pull their current level-triggered slices. Reconnects receive the latest whole slice.
///
/// # Implementing handlers
///
/// Implement methods with plain `async fn`; the returned future satisfies
/// the `Send` bound automatically.
///
/// **Unary and server-streaming requests** arrive as
/// [`ServiceRequest<'_, Req>`](::connectrpc::ServiceRequest): a zero-copy
/// view of the request plus its body, valid for the duration of the call.
/// Fields are read directly (`request.name` is a `&str` into the decoded
/// buffer) and the borrow may be held across `.await` points. Anything
/// that must outlive the call — `tokio::spawn`, channels, server state,
/// or data captured by a returned response stream — takes owned data:
/// call `request.to_owned_message()` (or copy the specific fields)
/// first.
///
/// **Client-streaming and bidi requests** arrive as
/// [`InboundStream<Req>`](::connectrpc::InboundStream) — a
/// `ServiceStream` of [`StreamMessage`](::connectrpc::StreamMessage)s.
/// Each item owns its decoded buffer and is `Send + 'static`, so items
/// can be buffered or moved into spawned tasks; read fields zero-copy
/// through the generated accessor methods (`item.name()`) or `.view()`,
/// convert with `.to_owned_message()`, or yield an item back unchanged —
/// `StreamMessage<M>` implements `Encodable<M>`.
///
/// Request types resolved through `extern_path` (e.g. well-known types
/// from another crate) use the same wrappers; the crate that owns the
/// type must be generated with buffa ≥ 0.8.0 and views enabled so the
/// backing `HasMessageView` impl exists.
///
/// The `impl Encodable<Out>` return bound accepts the owned `Out`, the
/// generated `OutView<'_>` / `OwnedOutView`,
/// [`MaybeBorrowed`](::connectrpc::MaybeBorrowed), or
/// [`PreEncoded`](::connectrpc::PreEncoded) for handlers that encode a
/// non-`'static` view internally and pass the bytes across the handler
/// boundary. View bodies are not emitted for output types mapped via
/// `extern_path` (the impl would be an orphan); return owned for
/// WKT/extern outputs.
///
/// Server-streaming and bidi-streaming methods return
/// `ServiceStream<impl Encodable<Out> + Send + use<Self>>`. The
/// `use<Self>` precise-capturing clause excludes `&self`'s lifetime and
/// the request's lifetime (unary methods use `use<'a, Self>` and may
/// borrow from `&self`), so stream items must be `'static` and cannot
/// borrow from the request. To stream view-encoded data, encode each
/// item inside the stream body and yield
/// [`PreEncoded`](::connectrpc::PreEncoded) — see its `# Streaming
/// example` doc.
#[allow(clippy::type_complexity)]
pub trait ControllerService: Send + Sync + 'static {
    /// Handle the PullSlices RPC.
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call (until the response stream is returned);
    /// message fields are read directly on it (zero-copy). Data the
    /// returned stream needs must be copied out or converted via
    /// `.to_owned_message()`.
    fn pull_slices(
        &self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<
            '_,
            crate::proto::henosis::v1::PullSlicesRequest,
        >,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            ::connectrpc::ServiceStream<
                impl ::connectrpc::Encodable<
                    crate::proto::henosis::v1::PullSlicesResponse,
                > + Send + use<Self>,
            >,
        >,
    > + Send;
    /// Handle the ReportSlice RPC.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call; message fields are read directly on it
    /// (zero-copy). The response cannot borrow from `request` — use
    /// `.to_owned_message()` (or copy the specific fields) for anything
    /// returned, stored, or moved into `tokio::spawn`.
    fn report_slice<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<
            '_,
            crate::proto::henosis::v1::ReportSliceRequest,
        >,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::henosis::v1::ReportSliceResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
}
/// Extension trait for registering a service implementation with a Router.
///
/// This trait is automatically implemented for all types that implement the service trait.
/// Prefer [`Router::add_service`](::connectrpc::Router::add_service) for
/// top-down registration; `register` remains available for compatibility
/// and cases where the service-first call shape is more convenient.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
///
/// let service = Arc::new(MyServiceImpl);
/// let router = service.register(Router::new());
/// ```
pub trait ControllerServiceExt: ControllerService {
    /// Register this service implementation with a Router.
    ///
    /// Takes ownership of the `Arc<Self>` and returns a new Router with
    /// this service's methods registered.
    fn register(
        self: ::std::sync::Arc<Self>,
        router: ::connectrpc::Router,
    ) -> ::connectrpc::Router;
}
impl<S: ControllerService> ControllerServiceExt for S {
    fn register(
        self: ::std::sync::Arc<Self>,
        router: ::connectrpc::Router,
    ) -> ::connectrpc::Router {
        router
            .route_view_server_stream::<
                _,
                _,
                crate::proto::henosis::v1::PullSlicesResponse,
            >(
                CONTROLLER_SERVICE_SERVICE_NAME,
                "PullSlices",
                ::connectrpc::view_streaming_handler_fn({
                    let svc = ::std::sync::Arc::clone(&self);
                    move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            crate::proto::henosis::v1::__buffa::view::PullSlicesRequestView<
                                'static,
                            >,
                        >|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                crate::proto::henosis::v1::PullSlicesRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.pull_slices(ctx, sreq).await
                        }
                    }
                }),
            )
            .with_spec(CONTROLLER_SERVICE_PULL_SLICES_SPEC)
            .route_view(
                CONTROLLER_SERVICE_SERVICE_NAME,
                "ReportSlice",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            crate::proto::henosis::v1::__buffa::view::ReportSliceRequestView<
                                'static,
                            >,
                        >,
                        format|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                crate::proto::henosis::v1::ReportSliceRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.report_slice(ctx, sreq)
                                .await?
                                .encode::<
                                    crate::proto::henosis::v1::ReportSliceResponse,
                                >(format)
                        }
                    })
                },
            )
            .with_spec(CONTROLLER_SERVICE_REPORT_SLICE_SPEC)
    }
}
/// Type-inference marker used by [`Router::add_service`](::connectrpc::Router::add_service).
#[doc(hidden)]
pub struct ControllerServiceRegisterMarker;
impl<S: ControllerService> ::connectrpc::ServiceRegister<ControllerServiceRegisterMarker>
for ::std::sync::Arc<S> {
    fn register_service(self, router: ::connectrpc::Router) -> ::connectrpc::Router {
        <S as ControllerServiceExt>::register(self, router)
    }
}
/// Monomorphic dispatcher for `ControllerService`.
///
/// Unlike `.register(Router)` which type-erases each method into an `Arc<dyn ErasedHandler>` stored in a `HashMap`, this struct dispatches via a compile-time `match` on method name: no vtable, no hash lookup.
///
/// # Example
///
/// ```rust,ignore
/// use connectrpc::ConnectRpcService;
///
/// let server = ControllerServiceServer::new(MyImpl);
/// let service = ConnectRpcService::new(server);
/// // hand `service` to axum/hyper as a fallback_service
/// ```
pub struct ControllerServiceServer<T> {
    inner: ::std::sync::Arc<T>,
}
impl<T: ControllerService> ControllerServiceServer<T> {
    /// Wrap a service implementation in a monomorphic dispatcher.
    pub fn new(service: T) -> Self {
        Self {
            inner: ::std::sync::Arc::new(service),
        }
    }
    /// Wrap an already-`Arc`'d service implementation.
    pub fn from_arc(inner: ::std::sync::Arc<T>) -> Self {
        Self { inner }
    }
}
impl<T> Clone for ControllerServiceServer<T> {
    fn clone(&self) -> Self {
        Self {
            inner: ::std::sync::Arc::clone(&self.inner),
        }
    }
}
impl<T: ControllerService> ::connectrpc::Dispatcher for ControllerServiceServer<T> {
    #[inline]
    fn lookup(
        &self,
        path: &str,
    ) -> Option<::connectrpc::dispatcher::codegen::MethodDescriptor> {
        let method = path.strip_prefix("henosis.v1.ControllerService/")?;
        match method {
            "PullSlices" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::server_streaming()
                        .with_spec(CONTROLLER_SERVICE_PULL_SLICES_SPEC),
                )
            }
            "ReportSlice" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(false)
                        .with_spec(CONTROLLER_SERVICE_REPORT_SLICE_SPEC),
                )
            }
            _ => None,
        }
    }
    fn call_unary(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::Payload,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::UnaryResult {
        let Some(method) = path.strip_prefix("henosis.v1.ControllerService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_unary(path);
        };
        let _ = (&ctx, &request, &format);
        match method {
            "ReportSlice" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        crate::proto::henosis::v1::ReportSliceRequest,
                    >(request.encoded()?, format)?;
                    let req: crate::proto::henosis::v1::__buffa::view::ReportSliceRequestView<
                        '_,
                    > = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        crate::proto::henosis::v1::ReportSliceRequest,
                    >::from_parts(&req, &body);
                    svc.report_slice(ctx, req)
                        .await?
                        .encode::<crate::proto::henosis::v1::ReportSliceResponse>(format)
                })
            }
            _ => ::connectrpc::dispatcher::codegen::unimplemented_unary(path),
        }
    }
    fn call_server_streaming(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        request: ::buffa::bytes::Bytes,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::StreamingResult {
        let Some(method) = path.strip_prefix("henosis.v1.ControllerService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_streaming(path);
        };
        let _ = (&ctx, &request, &format);
        match method {
            "PullSlices" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        crate::proto::henosis::v1::PullSlicesRequest,
                    >(request, format)?;
                    let req: crate::proto::henosis::v1::__buffa::view::PullSlicesRequestView<
                        '_,
                    > = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        crate::proto::henosis::v1::PullSlicesRequest,
                    >::from_parts(&req, &body);
                    let resp = svc.pull_slices(ctx, req).await?;
                    Ok(
                        resp
                            .map_body(|s| ::connectrpc::dispatcher::codegen::encode_response_stream::<
                                crate::proto::henosis::v1::PullSlicesResponse,
                                _,
                                _,
                            >(s, format)),
                    )
                })
            }
            _ => ::connectrpc::dispatcher::codegen::unimplemented_streaming(path),
        }
    }
    fn call_client_streaming(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        requests: ::connectrpc::dispatcher::codegen::RequestStream,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::UnaryResult {
        let Some(method) = path.strip_prefix("henosis.v1.ControllerService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_unary(path);
        };
        let _ = (&ctx, &requests, &format);
        match method {
            _ => ::connectrpc::dispatcher::codegen::unimplemented_unary(path),
        }
    }
    fn call_bidi_streaming(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        requests: ::connectrpc::dispatcher::codegen::RequestStream,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::StreamingResult {
        let Some(method) = path.strip_prefix("henosis.v1.ControllerService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_streaming(path);
        };
        let _ = (&ctx, &requests, &format);
        match method {
            _ => ::connectrpc::dispatcher::codegen::unimplemented_streaming(path),
        }
    }
}
/// Client for this service.
///
/// Generic over `T: ClientTransport`. For **gRPC** (HTTP/2), use
/// `Http2Connection` — it has honest `poll_ready` and composes with
/// `tower::balance` for multi-connection load balancing. For **Connect
/// over HTTP/1.1** (or unknown protocol), use `HttpClient`.
///
/// # Example (gRPC / HTTP/2)
///
/// ```rust,ignore
/// use connectrpc::client::{Http2Connection, ClientConfig};
/// use connectrpc::Protocol;
///
/// let uri: http::Uri = "http://localhost:8080".parse()?;
/// let conn = Http2Connection::connect_plaintext(uri.clone()).await?.shared(1024);
/// let config = ClientConfig::new(uri).with_protocol(Protocol::Grpc);
///
/// let client = ControllerServiceClient::new(conn, config);
/// let response = client.pull_slices(request).await?;
/// ```
///
/// # Example (Connect / HTTP/1.1 or ALPN)
///
/// ```rust,ignore
/// use connectrpc::client::{HttpClient, ClientConfig};
///
/// let http = HttpClient::plaintext();  // cleartext http:// only
/// let config = ClientConfig::new("http://localhost:8080".parse()?);
///
/// let client = ControllerServiceClient::new(http, config);
/// let response = client.pull_slices(request).await?;
/// ```
///
/// # Working with the response
///
/// Unary calls return [`UnaryResponse<OwnedView<FooView>>`](::connectrpc::client::UnaryResponse).
/// [`view()`](::connectrpc::client::UnaryResponse::view) borrows the response
/// message, so field access is zero-copy:
///
/// ```rust,ignore
/// let resp = client.pull_slices(request).await?;
/// let name: &str = resp.view().name;  // borrow into the response buffer
/// ```
///
/// If you need the owned struct (e.g. to store or pass by value), use
/// [`into_owned()`](::connectrpc::client::UnaryResponse::into_owned):
///
/// ```rust,ignore
/// let owned = client.pull_slices(request).await?.into_owned();
/// ```
///
/// [`into_view()`](::connectrpc::client::UnaryResponse::into_view) keeps the
/// zero-copy decoded body (an `OwnedView`) without copying; field access on it
/// goes through `.reborrow()`. Streaming responses yield one
/// [`StreamMessage`](::connectrpc::StreamMessage) per received message from
/// `.message().await` — read fields zero-copy through the generated accessor
/// methods (`msg.name()`) or `.view()`, or convert with `.to_owned_message()`.
#[derive(Clone)]
pub struct ControllerServiceClient<T> {
    transport: T,
    config: ::connectrpc::client::ClientConfig,
}
impl<T> ControllerServiceClient<T>
where
    T: ::connectrpc::client::ClientTransport,
    <T::ResponseBody as ::connectrpc::http_body::Body>::Error: ::std::fmt::Display,
{
    /// Create a new client with the given transport and configuration.
    pub fn new(transport: T, config: ::connectrpc::client::ClientConfig) -> Self {
        Self { transport, config }
    }
    /// Get the client configuration.
    pub fn config(&self) -> &::connectrpc::client::ClientConfig {
        &self.config
    }
    /// Get a mutable reference to the client configuration.
    pub fn config_mut(&mut self) -> &mut ::connectrpc::client::ClientConfig {
        &mut self.config
    }
    /// Call the PullSlices RPC. Sends a request to /henosis.v1.ControllerService/PullSlices.
    pub async fn pull_slices(
        &self,
        request: crate::proto::henosis::v1::PullSlicesRequest,
    ) -> Result<
        ::connectrpc::client::ServerStream<
            T::ResponseBody,
            crate::proto::henosis::v1::__buffa::view::PullSlicesResponseView<'static>,
        >,
        ::connectrpc::ConnectError,
    > {
        self.pull_slices_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the PullSlices RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn pull_slices_with_options(
        &self,
        request: crate::proto::henosis::v1::PullSlicesRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::ServerStream<
            T::ResponseBody,
            crate::proto::henosis::v1::__buffa::view::PullSlicesResponseView<'static>,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_server_stream(
                &self.transport,
                &self.config,
                CONTROLLER_SERVICE_SERVICE_NAME,
                "PullSlices",
                request,
                options,
            )
            .await
    }
    /// Call the ReportSlice RPC. Sends a request to /henosis.v1.ControllerService/ReportSlice.
    pub async fn report_slice(
        &self,
        request: crate::proto::henosis::v1::ReportSliceRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::henosis::v1::__buffa::view::ReportSliceResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.report_slice_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the ReportSlice RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn report_slice_with_options(
        &self,
        request: crate::proto::henosis::v1::ReportSliceRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::henosis::v1::__buffa::view::ReportSliceResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                CONTROLLER_SERVICE_SERVICE_NAME,
                "ReportSlice",
                request,
                options,
            )
            .await
    }
}
