use crate::auth::Auth;
use crate::errors::{GenericError, SendMessageError};
use crate::metrics::{self, GRPC_COUNTER};
use crate::pb::{
    self, hsmq_server, publish_response, subscription_response, Message, PublishResponse,
    SubscribeQueueRequest, SubscriptionResponse,
};
use crate::server::{
    self, Consumer, ConsumerSendResult, Envelop, GenericConsumer, HsmqServer, QueueCommand,
    Subscription,
};

use prometheus::core::{AtomicF64, GenericCounter, GenericGauge};
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::{error::Error, io::ErrorKind};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tokio_util::task::task_tracker::TaskTracker;
use tonic::Streaming;
use tonic::{transport::Server as TonicServer, Request, Response, Status};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

type HsmqResult<T> = Result<Response<T>, Status>;

pub struct GrpcService {
    addr: SocketAddr,
    task_tracker: TaskTracker,
}

impl GrpcService {
    pub fn new(addr: SocketAddr, task_tracker: TaskTracker) -> Self {
        Self { addr, task_tracker }
    }
    pub async fn run(&self, hsmq: HsmqServer, auth: Arc<Auth>, listener: Option<TcpListener>) {
        let task_tracker = self.task_tracker.clone();

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<hsmq_server::HsmqServer<HsmqServer>>()
            .await;

        let svc =
            hsmq_server::HsmqServer::with_interceptor(hsmq, move |req| auth.grpc_check_auth(req));
        log::info!("Run grpc on {:?}", &self.addr);

        let incoming = match listener {
            Some(l) => l,
            None => TcpListener::bind(self.addr).await.unwrap(),
        };

        if let Err(e) = TonicServer::builder()
            .add_service(health_service)
            .add_service(svc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(incoming),
                async move { task_tracker.wait().await },
            )
            .await
        {
            self.task_tracker.close();
            log::error!("Critical error with gRPC serve: {:?}", e);
        };
        log::info!("Stopped");
    }
}

#[derive(Debug)]
struct InnerConsumer<T> {
    in_tx: mpsc::UnboundedSender<server::Response>,
    out_tx: mpsc::Sender<Result<T, Status>>,
    q: server::GenericQueue,
    m_consume_ok: GenericCounter<AtomicF64>,
    m_consume_err: GenericCounter<AtomicF64>,
    m_buffer: GenericGauge<AtomicF64>,
    prefetch_semaphore: Semaphore,
    prefetch_count: usize,
    is_ackable: bool,
}

#[tonic::async_trait]
trait TInnerConsumer {
    async fn send_message(self, msg: Arc<Envelop>) -> Result<(), SendMessageError>;
    async fn send_fetch_timeout(&self, queue: String) -> Result<(), GenericError>;
}

#[tonic::async_trait]
impl TInnerConsumer for Arc<InnerConsumer<pb::SubscriptionResponse>> {
    #[tracing::instrument(parent = &msg.span, skip_all, level = "debug")]
    async fn send_message(self, msg: Arc<Envelop>) -> Result<(), SendMessageError> {
        let m = msg.message.clone();
        let resp = pb::SubscriptionResponse {
            kind: Some(subscription_response::Kind::Message(m)),
        };
        self.out_tx
            .send(Ok(resp))
            .await
            .map_err(|_| SendMessageError)
    }

    async fn send_fetch_timeout(&self, _queue: String) -> Result<(), GenericError> {
        unimplemented!("Subsciption api not supported timeout")
    }
}

#[tonic::async_trait]
impl TInnerConsumer for Arc<InnerConsumer<pb::Response>> {
    #[tracing::instrument(parent = &msg.span, skip_all, level = "debug")]
    async fn send_message(self, msg: Arc<Envelop>) -> Result<(), SendMessageError> {
        let mut meta = msg.meta.clone();
        meta.queue = self.q.get_name();
        let message = Some(msg.message.clone());
        let meta = Some(meta);
        let message = pb::MessageWithMeta { message, meta };
        let resp = pb::Response {
            kind: Some(pb::response::Kind::Message(message)),
        };
        self.out_tx
            .send(Ok(resp))
            .await
            .map_err(|_| SendMessageError)
    }

    async fn send_fetch_timeout(&self, queue: String) -> Result<(), GenericError> {
        let fmt = pb::FetchMessageTimeout { queue };
        let resp = pb::Response {
            kind: Some(pb::response::Kind::FetchMessageTimeout(fmt)),
        };
        self.out_tx.send(Ok(resp)).await?;
        Ok(())
    }
}

#[derive(Debug)]
struct GrpcConsumer<T> {
    id: uuid::Uuid,
    inner: Arc<InnerConsumer<T>>,
    span: Option<tracing::Span>,
    deadline: Option<SystemTime>,
}

impl<T> Clone for GrpcConsumer<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            span: Some(tracing::debug_span!("grpc::consume")),
            inner: self.inner.clone(),
            deadline: self.deadline,
        }
    }
}

impl<T> GrpcConsumer<T> {
    fn new(
        id: Uuid,
        out_tx: mpsc::Sender<Result<T, Status>>,
        in_tx: mpsc::UnboundedSender<server::Response>,
        q: server::GenericQueue,
        prefetch_count: usize,
        autoack: bool,
    ) -> Self {
        let m_buffer = metrics::GRPC_GAUGE.with_label_values(&["buffer"]);
        let m_consume_ok = metrics::GRPC_COUNTER.with_label_values(&["consume", "ok"]);
        let m_consume_err = metrics::GRPC_COUNTER.with_label_values(&["consume", "error"]);
        let prefetch_semaphore = Semaphore::new(prefetch_count);
        let inner = InnerConsumer {
            in_tx,
            out_tx,
            q,
            m_buffer,
            m_consume_ok,
            m_consume_err,
            prefetch_semaphore,
            prefetch_count,
            is_ackable: !autoack,
        };
        Self {
            id,
            inner: Arc::new(inner),
            span: None,
            deadline: None,
        }
    }

    fn new_box(
        id: Uuid,
        out_tx: mpsc::Sender<Result<T, Status>>,
        in_tx: mpsc::UnboundedSender<server::Response>,
        q: server::GenericQueue,
        prefetch_count: usize,
        autoack: bool,
    ) -> Box<Self> {
        Box::new(Self::new(id, out_tx, in_tx, q, prefetch_count, autoack))
    }
}

#[tonic::async_trait]
impl<T> Consumer for GrpcConsumer<T>
where
    T: std::fmt::Debug + std::marker::Send + 'static,
    Arc<InnerConsumer<T>>: TInnerConsumer,
{
    fn get_id(&self) -> uuid::Uuid {
        self.id
    }

    fn get_prefetch_count(&self) -> usize {
        self.inner.prefetch_count
    }

    fn is_ackable(&self) -> bool {
        self.inner.is_ackable
    }

    fn set_deadline(&mut self, deadline: SystemTime) {
        self.deadline = Some(deadline);
    }

    fn is_dead(&self) -> bool {
        self.deadline
            .map(|x| x < SystemTime::now())
            .unwrap_or_default()
    }

    async fn send_timeout(&self, queue: String) -> Result<(), GenericError> {
        self.inner.send_fetch_timeout(queue).await
    }

    fn generic_clone(&self) -> GenericConsumer {
        let result = self.clone();
        Box::new(result) as GenericConsumer
    }

    fn get_current_tracing_span(&self) -> tracing::Span {
        if let Some(ref span) = self.span {
            span.clone()
        } else {
            tracing::trace_span!("grpc::consumer")
        }
    }

    #[tracing::instrument(parent = &msg.span, skip_all, level = "debug")]
    fn send(
        &self,
        msg: Arc<Envelop>,
        tasks: &mut JoinSet<ConsumerSendResult>,
        unack: &mut server::UnAck,
    ) -> Option<Arc<Envelop>> {
        let consumer = self.clone();
        let prefetch = self.inner.prefetch_count > 0;
        let timeout = unack.timeout;
        if prefetch {
            unack.insert(msg.clone());
            self.inner.prefetch_semaphore.forget_permits(1);
        }
        tasks.spawn(async move {
            if consumer.send_message(msg.clone()).await.is_err() {
                ConsumerSendResult::RequeueAck(msg)
            } else if prefetch {
                tokio::select! {
                    _ = consumer.inner.prefetch_semaphore.acquire() => ConsumerSendResult::Consumer(consumer.id),
                    _ = tokio::time::sleep(timeout) => ConsumerSendResult::AckTimeout(msg),
                }
            } else {
                ConsumerSendResult::FetchDone
            }
        });
        None
    }

    #[tracing::instrument(parent = &msg.span, skip_all, level = "debug")]
    async fn send_message(&self, msg: Arc<Envelop>) -> Result<(), SendMessageError> {
        let inner = self.inner.clone();
        let result = inner.send_message(msg).await;
        result
            .inspect(|_| self.inner.m_consume_ok.inc())
            .inspect_err(|_| self.inner.m_consume_err.inc())
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn ack(&mut self, msg_id: String, unack: &mut server::UnAck) {
        if self.inner.prefetch_count > 0 {
            self.inner.prefetch_semaphore.add_permits(1);
        }
        unack.remove(&msg_id, false);
    }

    #[tracing::instrument(skip_all, level = "debug")]
    async fn send_resp(&self, resp: server::Response) -> Result<(), GenericError> {
        self.inner.in_tx.send(resp)?;
        self.inner.m_buffer.inc();
        Ok(())
    }

    async fn stop(&self) {
        let queue_name = self.inner.q.get_name();
        let _ = self
            .send_resp(server::Response::StopConsume(queue_name))
            .await;
    }
}

#[tonic::async_trait]
impl hsmq_server::Hsmq for HsmqServer {
    #[tracing::instrument(name = "publish", skip_all, level = "debug")]
    async fn publish(&self, request: Request<Message>) -> HsmqResult<PublishResponse> {
        if self.task_tracker.is_closed() {
            return Err(Status::cancelled("shutdown"));
        }
        let message = request.into_inner();
        let topic = message.topic.clone();
        let envelop = Envelop::new(message).with_generated_id();
        tracing::Span::current().set_parent(envelop.span.context());

        if let Some(subscription) = self.subscriptions.get(&topic) {
            GRPC_COUNTER.with_label_values(&["publish", "ok"]).inc();
            let kind = Some(publish_response::Kind::MessageMeta(envelop.meta.clone()));
            subscription.publish(envelop.with_generated_id()).await;
            Ok(Response::new(PublishResponse { kind }))
        } else {
            GRPC_COUNTER.with_label_values(&["publish", "error"]).inc();
            Err(Status::not_found("Subscribers not found"))
        }
    }

    async fn publish_qos0(
        &self,
        request: tonic::Request<tonic::Streaming<Message>>,
    ) -> HsmqResult<PublishResponse> {
        let m_publish_ok = GRPC_COUNTER.with_label_values(&["publish", "ok"]);
        let m_publish_err = GRPC_COUNTER.with_label_values(&["publish", "error"]);
        let mut in_stream = request.into_inner();

        let mut count = 0;
        while let Some(result) = in_stream.next().await {
            if self.task_tracker.is_closed() {
                return Err(Status::cancelled("shutdown"));
            };
            match result {
                Ok(message) => {
                    let span = tracing::trace_span!("publish");
                    let _ = span.enter();
                    let topic = message.topic.clone();
                    let envelop = Envelop::new(message).with_generated_id();
                    span.set_parent(envelop.span.context());

                    if let Some(subscription) = self.subscriptions.get(&topic) {
                        subscription.publish(envelop).await;
                        m_publish_ok.inc();
                        count += 1;
                    } else {
                        m_publish_err.inc();
                        return Err(Status::not_found("Subscribers not found"));
                    }
                }
                Err(err) => {
                    if let Some(io_err) = match_for_io_error(&err) {
                        if io_err.kind() == ErrorKind::BrokenPipe {
                            log::error!("Client disconnected: broken pipe");
                            break;
                        }
                    }
                }
            }
        }
        let kind = Some(publish_response::Kind::Count(count));
        Ok(Response::new(PublishResponse { kind }))
    }

    type SubscribeQueueStream =
        Pin<Box<dyn Stream<Item = Result<SubscriptionResponse, Status>> + Send>>;

    async fn subscribe_queue(
        &self,
        req: Request<SubscribeQueueRequest>,
    ) -> HsmqResult<Self::SubscribeQueueStream> {
        let (response_tx, response_rx) = mpsc::channel(1);
        let (server_tx, _) = mpsc::unbounded_channel::<server::Response>();

        log::info!(
            "Subscribe queue {:?} {:?}",
            req.remote_addr(),
            req.local_addr(),
        );
        let sqr = req.into_inner();
        let consumer_id = Uuid::now_v7();

        for queue_name in sqr.queues.iter() {
            if let Some(queue) = self.queues.get(queue_name) {
                let consumer = GrpcConsumer::new_box(
                    consumer_id,
                    response_tx.clone(),
                    server_tx.clone(),
                    queue.clone(),
                    0,
                    false,
                );
                match queue.subscribe(consumer).await {
                    Ok(_) => {}
                    Err(e) => log::error!("Consume start error {:?}", e),
                }
            }
        }

        let output_stream = ReceiverStream::new(response_rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::SubscribeQueueStream
        ))
    }

    type StreamingStream = Pin<Box<dyn Stream<Item = Result<pb::Response, tonic::Status>> + Send>>;

    async fn streaming(
        &self,
        request: tonic::Request<tonic::Streaming<pb::Request>>,
    ) -> HsmqResult<Self::StreamingStream> {
        let (response_tx, response_rx) = mpsc::channel::<Result<pb::Response, Status>>(1);
        let in_stream = request.into_inner();

        GrpcStreaming::spawn(
            in_stream,
            response_tx,
            self.queues.clone(),
            self.subscriptions.clone(),
        );

        let output_stream = ReceiverStream::new(response_rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::StreamingStream
        ))
    }
}

struct GrpcStreaming {
    consumer_id: Uuid,
    in_stream: Streaming<pb::Request>,
    out_tx: mpsc::Sender<Result<pb::Response, Status>>,
    subscriptions: BTreeMap<String, Subscription>,
    subs: HashMap<String, server::GenericSubscriber>,
    queues: HashMap<String, server::GenericQueue>,
    server_tx: mpsc::UnboundedSender<server::Response>,
    server_rx: mpsc::UnboundedReceiver<server::Response>,
    m_publish_ok: GenericCounter<AtomicF64>,
    m_publish_err: GenericCounter<AtomicF64>,
}

impl GrpcStreaming {
    fn spawn(
        in_stream: Streaming<pb::Request>,
        out_tx: mpsc::Sender<Result<pb::Response, Status>>,
        queues: HashMap<String, server::GenericQueue>,
        subscriptions: BTreeMap<String, Subscription>,
    ) {
        let (server_tx, server_rx) = mpsc::unbounded_channel::<server::Response>();

        let subs = HashMap::new();

        let consumer_id = Uuid::now_v7();

        let m_publish_ok = GRPC_COUNTER.with_label_values(&["publish", "ok"]);
        let m_publish_err = GRPC_COUNTER.with_label_values(&["publish", "error"]);

        let s = Self {
            consumer_id,
            out_tx,
            in_stream,
            subs,
            queues,
            subscriptions,
            server_tx,
            server_rx,
            m_publish_ok,
            m_publish_err,
        };

        tokio::spawn(s.run_loop());
    }
    async fn run_loop(mut self) {
        let m_buffer = metrics::GRPC_GAUGE.with_label_values(&["buffer"]);
        loop {
            tokio::select! {
                result = self.in_stream.next() => {
                    match result {
                        Some(result) => match result {
                            Ok(request) => {
                                if let Some(kind) = request.kind {
                                    if let Err(e) = self.req_kind(kind).await {
                                        log::error!("Error kind of request {:?}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                log::debug!("Error in stream {}", e);
                                self.consume_stop().await;
                                drop(self);
                                break;
                            }
                        }
                        None => {
                            tracing::debug!("Streaming stop");
                            self.consume_stop().await;
                            drop(self);
                            break;
                        }
                    }
                }
                Some(result) = self.server_rx.recv() => {
                    m_buffer.dec();
                    match result {
                        server::Response::StartConsume(queue_name) => {
                            if !self.subs.contains_key(&queue_name) {
                                log::error!("Start consume unknown queue {}", queue_name);
                            }
                        }
                        server::Response::StopConsume(queue_name) => {
                            self.subs.remove(&queue_name);
                        }
                        server::Response::GracefulShutdown(queue_name) => {
                            self.subs.remove(&queue_name);
                            if self.subs.is_empty() {
                                log::info!("Shutdown channel");
                                return;
                            }
                        }
                    };
                }
            }
        }
    }

    async fn consume_stop(&mut self) {
        for queue in self.subs.values() {
            let _ = queue
                .send(QueueCommand::ConsumeStop(self.consumer_id))
                .await
                .is_ok();
        }
    }

    async fn req_kind(&mut self, kind: pb::request::Kind) -> Result<(), GenericError> {
        match kind {
            pb::request::Kind::PublishMessage(pb::PublishMessage {
                message: Some(message),
                qos,
                request_id,
            }) => {
                let span = tracing::trace_span!("publish");
                let _ = span.enter();
                let topic = message.topic.clone();
                let envelop = Envelop::new(message).with_generated_id();
                span.set_parent(envelop.span.context());

                if let Some(subscription) = self.subscriptions.get(&topic) {
                    subscription.publish(envelop).await;
                    self.m_publish_ok.inc();
                    if qos == 1 {
                        let kind = Some(pb::response::Kind::PubAck(pb::PubAck { request_id }));
                        self.out_tx.send(Ok(pb::Response { kind })).await?;
                    }
                } else {
                    self.m_publish_err.inc();
                    let m = "Subscribers not found";
                    let kind = Some(pb::response::Kind::PubError(pb::PubError {
                        request_id,
                        topic,
                        info: m.to_string(),
                    }));
                    self.out_tx.send(Ok(pb::Response { kind })).await?;
                    return Err(Status::not_found(m))?;
                }
            }
            pb::request::Kind::SubscribeQueue(pb::SubscribeQueue {
                queues,
                prefetch_count,
            }) => {
                for queue_name in queues.iter() {
                    if let Some(queue) = self.queues.get_mut(queue_name) {
                        let q = queue.generic_clone();
                        let consumer = GrpcConsumer::new_box(
                            self.consumer_id,
                            self.out_tx.clone(),
                               self.server_tx.clone(),
                            q,
                            prefetch_count as usize,
                            false,
                        );
                        let q = queue.subscribe(consumer).await?;
                        self.subs.insert(queue_name.clone(), q);
                    }
                }
            }
            pb::request::Kind::MessageAck(
                ref ack @ pb::MessageAck {
                    meta:
                        Some(pb::MessageMeta {
                            ref id,
                            ref queue,
                            ref shard,
                        }),
                },
            ) => {
                if let Some(subscriber) = self.subs.get_mut(queue) {
                    if let Err(e) = subscriber.ack(id.clone(), shard.clone()).await {
                        tracing::error!(error = e, "Unexpected queue error");
                    };
                }
                let kind = Some(pb::response::Kind::MessageAck(ack.clone()));
                if let Err(e) = self.out_tx.send(Ok(pb::Response { kind })).await {
                    tracing::debug!(
                        error = &e as &dyn std::error::Error,
                        "Error send MessageAck",
                    );
                }
            }
            pb::request::Kind::MessageRequeue(_) => todo!(),
            pb::request::Kind::FetchMessage(pb::FetchMessage {
                queue,
                timeout,
                autoack,
            }) => {
                let deadline = SystemTime::now() + Duration::from_secs_f32(timeout);
                let fetch_result = if let Some(subscriber) = self.subs.get_mut(&queue) {
                    subscriber.fetch(deadline).await.map_err(|e| {
                        tracing::warn!("Unexpected error on queue {} {:?}", queue, e);
                        pb::FetchMessageError {
                            queue,
                            info: "Error while fetch from queue".to_string(),
                        }
                    })
                } else if let Some(qu) = self.queues.get_mut(&queue) {
                    let q = qu.generic_clone();
                    let consumer = GrpcConsumer::new_box(
                        self.consumer_id,
                        self.out_tx.clone(),
                        self.server_tx.clone(),
                        q,
                        0,
                        autoack,
                    );
                    let mut subscriber = qu.subscribe(consumer).await?;
                    let r = subscriber.fetch(deadline).await;
                    self.subs.insert(queue.clone(), subscriber);
                    r.map_err(|e| {
                        tracing::warn!("Unexpected error on queue {} {:?}", queue, e);
                        pb::FetchMessageError {
                            queue,
                            info: "Error while fetch from queue".to_string(),
                        }
                    })
                } else {
                    Err(pb::FetchMessageError {
                        queue,
                        info: "Queue not found".to_string(),
                    })
                };
                if let Err(e) = fetch_result {
                    let kind = Some(pb::response::Kind::FetchMessageError(e));
                    self.out_tx.send(Ok(pb::Response { kind })).await?;
                };
            }
            pb::request::Kind::Ping(_) => {
                let kind = Some(pb::response::Kind::Pong(pb::Pong {}));
                self.out_tx.send(Ok(pb::Response { kind })).await?;
            }
            k => log::error!("Unexpected for streaming kind {:?}", k),
        };
        Ok(())
    }
}

fn match_for_io_error(err_status: &Status) -> Option<&std::io::Error> {
    let mut err: &(dyn Error + 'static) = err_status;

    loop {
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            return Some(io_err);
        }

        // h2::Error do not expose std::io::Error with `source()`
        // https://github.com/hyperium/h2/pull/462
        if let Some(h2_err) = err.downcast_ref::<h2::Error>() {
            if let Some(io_err) = h2_err.get_io() {
                return Some(io_err);
            }
        }

        err = match err.source() {
            Some(err) => err,
            None => return None,
        };
    }
}

#[cfg(test)]
mod tests {
    use tokio_stream::StreamExt;
    use crate::pb::{hsmq_server::Hsmq, Message};
    use crate::server::{HsmqServer, InMemoryQueue, Subscription};
    use crate::config;

    mod tonic_mock {
        mod mock {
            use bytes::{Buf, BufMut, Bytes, BytesMut};
            use http_body::{Body, Frame};
            use prost::Message;
            use std::{
                collections::VecDeque,
                marker::PhantomData,
                pin::Pin,
                task::{Context, Poll},
            };
            use tonic::{
                codec::{DecodeBuf, Decoder},
                Status,
            };

            #[derive(Clone)]
            pub struct MockBody {
                data: VecDeque<Bytes>,
            }

            impl MockBody {
                pub fn new(data: Vec<impl Message>) -> Self {
                    let mut queue: VecDeque<Bytes> = VecDeque::with_capacity(16);
                    for msg in data {
                        let buf = Self::encode(msg);
                        queue.push_back(buf);
                    }

                    MockBody { data: queue }
                }

                pub fn is_empty(&self) -> bool {
                    self.data.is_empty()
                }

                // see: https://github.com/hyperium/tonic/blob/1b03ece2a81cb7e8b1922b3c3c1f496bd402d76c/tonic/src/codec/encode.rs#L52
                fn encode(msg: impl Message) -> Bytes {
                    let mut buf = BytesMut::with_capacity(256);

                    buf.reserve(5);
                    unsafe {
                        buf.advance_mut(5);
                    }
                    msg.encode(&mut buf).unwrap();
                    {
                        let len = buf.len() - 5;
                        let mut buf = &mut buf[..5];
                        buf.put_u8(0); // byte must be 0, reserve doesn't auto-zero
                        buf.put_u32(len as u32);
                    }
                    buf.freeze()
                }
            }

            impl Body for MockBody {
                type Data = Bytes;
                type Error = Status;

                fn poll_frame(
                    mut self: Pin<&mut Self>,
                    _: &mut Context<'_>,
                ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
                    if !self.is_end_stream() {
                        let msg = self.data.pop_front().unwrap();
                        Poll::Ready(Some(Ok(Frame::data(msg))))
                    } else {
                        Poll::Ready(None)
                    }
                }

                fn is_end_stream(&self) -> bool {
                    self.is_empty()
                }

            }
            /// A [`Decoder`] that knows how to decode `U`.
            #[derive(Debug, Clone, Default)]
            pub struct ProstDecoder<U>(PhantomData<U>);

            impl<U> ProstDecoder<U> {
                pub fn new() -> Self {
                    Self(PhantomData)
                }
            }

            impl<U: Message + Default> Decoder for ProstDecoder<U> {
                type Item = U;
                type Error = Status;

                fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
                    let item = Message::decode(buf.chunk())
                        .map(Some)
                        .map_err(|e| Status::internal(e.to_string()))?;

                    buf.advance(buf.chunk().len());
                    Ok(item)
                }
            }
        }

        use futures::{Stream, StreamExt};
        use prost::Message;
        use std::pin::Pin;
        use tonic::{Request, Response, Status, Streaming};

        pub use mock::{MockBody, ProstDecoder};
        pub type StreamResponseInner<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;
        pub type StreamResponse<T> = Response<StreamResponseInner<T>>;

        pub fn streaming_request<T>(messages: Vec<T>) -> Request<Streaming<T>>
        where
            T: Message + Default + 'static,
        {
            let body = MockBody::new(messages);
            let decoder: ProstDecoder<T> = ProstDecoder::new();
            let stream = Streaming::new_request(decoder, body, None, None);

            Request::new(stream)
        }

        pub async fn process_streaming_response<T, F>(response: StreamResponse<T>, f: F)
        where
            T: Message + Default + 'static,
            F: Fn(Result<T, Status>, usize),
        {
            let mut i: usize = 0;
            let mut messages = response.into_inner();
            while let Some(v) = messages.next().await {
                f(v, i);
                i += 1;
            }
        }
    }

    #[tokio::test]
    async fn srv_publish_no_subs() {
        let srv = HsmqServer::default();
        let msg = Message::default();
        let req = tonic::Request::new(msg);
        let result = srv.publish(req).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn srv_publish_with_sub() {
        let mut srv = HsmqServer::default();
        let sub = Subscription::new();
        srv.subscriptions.insert("".to_string(), sub);
        let msg = Message::default();
        let req = tonic::Request::new(msg);
        let result = srv.publish(req).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn consume_streaming() {
        let queue_name = "a".to_string();
        let topic = "a.b".to_string();

        let mut srv = HsmqServer::default();
        let subscriptions = &mut srv.subscriptions;
        let queues = &mut srv.queues;

        let cfg_queue = config::InMemoryQueue{
            name: queue_name.clone(),
            topics: vec![topic.clone()],
            limit: Some(99),
            ack_timeout: config::Duration::Seconds{s: 2f32 },
            prefetch_count: 1,
        };

        let q = InMemoryQueue::new_generic(cfg_queue.clone(), srv.task_tracker.clone());
        let sub = subscriptions.entry(topic.clone()).or_insert_with(Subscription::new);
        sub.subscribe(q.clone());
        queues.insert(queue_name.clone(), q);

        let mut msg = Message::default();
        msg.topic.clone_from(&topic);
        let req = tonic::Request::new(msg);
        let result = srv.publish(req).await;
        assert!(result.is_ok());

        let cmd = crate::pb::SubscribeQueue {
            queues: vec![queue_name],
            prefetch_count: 1,
        };
        let kind = Some(crate::pb::request::Kind::SubscribeQueue(cmd));
        let req = tonic_mock::streaming_request(vec![crate::pb::Request { kind }]);
        let mut result = srv.streaming(req).await.unwrap().into_inner();

        assert!(result.next().await.unwrap().is_ok());
        assert!(result.next().await.is_none());
    }
}
