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
    pub async fn run(&self, hsmq: HsmqServer, auth: Arc<Auth>) {
        let task_tracker = self.task_tracker.clone();
        let svc =
            hsmq_server::HsmqServer::with_interceptor(hsmq, move |req| auth.grpc_check_auth(req));
        log::info!("Run grpc on {:?}", &self.addr);
        if let Err(e) = TonicServer::builder()
            .add_service(svc)
            .serve_with_shutdown(self.addr, async move { task_tracker.wait().await })
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
}

#[tonic::async_trait]
impl TInnerConsumer for Arc<InnerConsumer<pb::SubscriptionResponse>> {
    #[tracing::instrument(parent = &msg.span, skip_all)]
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
}

#[tonic::async_trait]
impl TInnerConsumer for Arc<InnerConsumer<pb::Response>> {
    #[tracing::instrument(parent = &msg.span, skip_all)]
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
            span: Some(tracing::info_span!("grpc::consume")),
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

    async fn send_timeout(&self, _msg: Arc<Envelop>) -> Result<(), SendMessageError> {
        Ok(())
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

    #[tracing::instrument(parent = &msg.span, skip_all)]
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

    #[tracing::instrument(parent = &msg.span, skip_all)]
    async fn send_message(&self, msg: Arc<Envelop>) -> Result<(), SendMessageError> {
        let inner = self.inner.clone();
        let result = inner.send_message(msg).await;
        result
            .inspect(|_| self.inner.m_consume_ok.inc())
            .inspect_err(|_| self.inner.m_consume_err.inc())
    }

    #[tracing::instrument(skip_all)]
    fn ack(&mut self, msg_id: String, unack: &mut server::UnAck) {
        self.inner.prefetch_semaphore.add_permits(1);
        unack.remove(&msg_id, false);
    }

    #[tracing::instrument(skip_all)]
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
    #[tracing::instrument(name = "publish", skip_all)]
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
                Some(result) = self.in_stream.next() => {
                    match result {
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
                        let kind = Some(pb::response::Kind::PubAck(pb::PubAck {}));
                        self.out_tx.send(Ok(pb::Response { kind })).await?;
                    }
                } else {
                    self.m_publish_err.inc();
                    return Err(Status::not_found("Subscribers not found"))?;
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
            pb::request::Kind::MessageAck(pb::MessageAck {
                meta: Some(pb::MessageMeta { id, queue, shard }),
            }) => {
                if let Some(subscriber) = self.subs.get_mut(&queue) {
                    if let Err(e) = subscriber.ack(id, shard).await {
                        log::error!("Unexpected queue error {:?}", e);
                    };
                }
            }
            pb::request::Kind::MessageRequeue(_) => todo!(),
            pb::request::Kind::FetchMessage(pb::FetchMessage {
                queue,
                timeout,
                autoack,
            }) => {
                let deadline = SystemTime::now() + Duration::from_secs_f32(timeout);
                if let Some(subscriber) = self.subs.get_mut(&queue) {
                    if let Err(e) = subscriber.fetch(deadline).await {
                        log::error!("Unexpected queue error {:?}", e);
                    };
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
                    if let Err(e) = subscriber.fetch(deadline).await {
                        log::error!("Unexpected queue error {:?}", e);
                    };
                    self.subs.insert(queue.clone(), subscriber);
                }
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
    use crate::pb::{hsmq_server::Hsmq, Message};
    use crate::server::{HsmqServer, Subscription};

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
}
