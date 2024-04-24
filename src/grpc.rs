use crate::errors::GenericError;
use crate::metrics::{self, GRPC_COUNTER};
use crate::pb::{
    self, hsmq_server, publish_response, subscription_response, Message, MessageWithId,
    PublishResponse, SubscribeQueueRequest, SubscriptionResponse,
};
use crate::server::{self, Consumer, Envelop, HsmqServer, Queue, QueueCommand};
use prometheus::core::{AtomicF64, GenericCounter, GenericGauge};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::{error::Error, io::ErrorKind};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tokio_util::task::task_tracker::TaskTracker;
use tonic::Streaming;
use tonic::{metadata::MetadataValue, transport::Server as TonicServer, Request, Response, Status};
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
    pub async fn run(&self, hsmq: HsmqServer) {
        let task_tracker = self.task_tracker.clone();
        let svc = hsmq_server::HsmqServer::with_interceptor(hsmq, check_auth);
        log::info!("Run grpc on {:?}", &self.addr);
        TonicServer::builder()
            .add_service(svc)
            .serve_with_shutdown(self.addr, async move { task_tracker.wait().await })
            .await
            .unwrap();
        log::info!("Stopped");
    }
}

#[derive(Debug)]
struct GrpcConsumer<T> {
    id: uuid::Uuid,
    out_tx: mpsc::Sender<Result<T, Status>>,
    in_tx: mpsc::UnboundedSender<server::Response>,
    q: Queue,
    ack: bool,
    m_consume_ok: GenericCounter<AtomicF64>,
    m_buffer: GenericGauge<AtomicF64>,
}

impl<T> GrpcConsumer<T> {
    fn new(
        id: Uuid,
        out_tx: mpsc::Sender<Result<T, Status>>,
        in_tx: mpsc::UnboundedSender<server::Response>,
        q: Queue,
        ack: bool,
    ) -> Self {
        let m_buffer = metrics::GRPC_GAUGE.with_label_values(&["buffer"]);
        let m_consume_ok = metrics::GRPC_COUNTER.with_label_values(&["consume", "ok"]);

        Self {
            id,
            out_tx,
            in_tx,
            q,
            ack,
            m_consume_ok,
            m_buffer,
        }
    }
    fn new_box(
        id: Uuid,
        out_tx: mpsc::Sender<Result<T, Status>>,
        in_tx: mpsc::UnboundedSender<server::Response>,
        q: Queue,
        ack: bool,
    ) -> Box<Self> {
        Box::new(Self::new(id, out_tx, in_tx, q, ack))
    }
}

#[tonic::async_trait]
impl Consumer for GrpcConsumer<pb::SubscriptionResponse> {
    fn get_id(&self) -> uuid::Uuid {
        self.id
    }
    fn with_ack(&self) -> bool {
        self.ack
    }
    async fn send_id(&self, _msg: Arc<Envelop>, _id: String) -> Result<(), GenericError> {
        Ok(())
    }
    async fn send(&self, msg: Arc<Envelop>) -> Result<(), GenericError> {
        let msg = msg.message.clone();
        let resp = pb::SubscriptionResponse {
            kind: Some(subscription_response::Kind::Message(msg)),
        };
        self.out_tx.send(Result::<_, Status>::Ok(resp)).await?;
        self.m_consume_ok.inc();
        Ok(())
    }
    async fn send_resp(&self, resp: server::Response) -> Result<(), GenericError> {
        self.in_tx.send(resp)?;
        self.m_buffer.inc();
        Ok(())
    }
    async fn stop(&self) {
        let queue_name = self.q.name.clone();
        let _ = self
            .send_resp(server::Response::StopConsume(queue_name))
            .await;
    }
}

#[tonic::async_trait]
impl Consumer for GrpcConsumer<pb::Response> {
    fn get_id(&self) -> uuid::Uuid {
        self.id
    }
    fn with_ack(&self) -> bool {
        self.ack
    }
    async fn send(&self, _msg: Arc<Envelop>) -> Result<(), GenericError> {
        Ok(())
    }
    async fn send_id(&self, msg: Arc<Envelop>, id: String) -> Result<(), GenericError> {
        let message = Some(msg.message.clone());
        let msg = MessageWithId {
            message,
            id,
            queue: self.q.name.clone(),
        };
        let resp = pb::Response {
            kind: Some(pb::response::Kind::Message(msg)),
        };
        self.out_tx.send(Result::<_, Status>::Ok(resp)).await?;
        self.m_consume_ok.inc();
        Ok(())
    }
    async fn send_resp(&self, resp: server::Response) -> Result<(), GenericError> {
        self.in_tx.send(resp)?;
        self.m_buffer.inc();
        Ok(())
    }
    async fn stop(&self) {
        let queue_name = self.q.name.clone();
        let _ = self
            .send_resp(server::Response::StopConsume(queue_name))
            .await;
    }
}

#[tonic::async_trait]
impl hsmq_server::Hsmq for HsmqServer {
    async fn publish(&self, request: Request<Message>) -> HsmqResult<PublishResponse> {
        if self.task_tracker.is_closed() {
            return Err(Status::cancelled("shutdown"));
        }
        let message = request.into_inner();
        let topic = message.topic.clone();
        let envelop = Envelop::new(message);
        let msg_id = envelop.gen_msg_id().to_string();
        if let Some(subscription) = self.subscriptions.get(&topic) {
            GRPC_COUNTER.with_label_values(&["publish", "ok"]).inc();
            subscription.send(envelop).await;
            let kind = Some(publish_response::Kind::MsgId(msg_id));
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
                    let topic = message.topic.clone();
                    let envelop = Envelop::new(message);
                    if let Some(subscription) = self.subscriptions.get(&topic) {
                        subscription.send(envelop).await;
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
            req.local_addr()
        );
        let sqr = req.into_inner();
        let consumer_id = Uuid::now_v7();

        for queue_name in sqr.queue.iter() {
            if let Some(queue) = self.queues.get(queue_name) {
                let consumer = GrpcConsumer::new_box(
                    consumer_id,
                    response_tx.clone(),
                    server_tx.clone(),
                    queue.clone(),
                    false,
                );
                let cmd = QueueCommand::ConsumeStart(consumer);
                match queue.send(cmd).await {
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

        GrpcStreaming::spawn(in_stream, response_tx, self.queues.clone());

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
    subs: HashMap<String, server::Queue>,
    queues: HashMap<String, Queue>,
    server_tx: mpsc::UnboundedSender<server::Response>,
    server_rx: mpsc::UnboundedReceiver<server::Response>,
}

impl GrpcStreaming {
    fn spawn(
        in_stream: Streaming<pb::Request>,
        out_tx: mpsc::Sender<Result<pb::Response, Status>>,
        queues: HashMap<String, Queue>,
    ) {
        let (server_tx, server_rx) = mpsc::unbounded_channel::<server::Response>();

        let subs = HashMap::new();

        let consumer_id = Uuid::now_v7();

        let s = Self {
            consumer_id,
            out_tx,
            in_stream,
            subs,
            queues,
            server_tx,
            server_rx,
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
                            log::error!("Error in stream {}", e);
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
            pb::request::Kind::SubscribeQueue(pb::SubscribeQueueRequest { queue }) => {
                for queue_name in queue.iter() {
                    if let Some(queue) = self.queues.get(queue_name) {
                        let consumer = GrpcConsumer::new_box(
                            self.consumer_id,
                            self.out_tx.clone(),
                            self.server_tx.clone(),
                            queue.clone(),
                            true,
                        );
                        let cmd = QueueCommand::ConsumeStart(consumer);
                        queue.send(cmd).await?;
                        self.subs.insert(queue_name.clone(), queue.clone());
                    }
                }
            }
            pb::request::Kind::MessageAck(pb::MessageAck { msg_id, queue }) => {
                if let Some(queue) = self.queues.get(&queue) {
                    let cmd = QueueCommand::MsgAck(msg_id);
                    if let Err(e) = queue.send(cmd).await {
                        log::error!("Unexpected queue error {:?}", e);
                    };
                }
            }
            k => log::error!("Unexpected for streaming kind {:?}", k),
        };
        Ok(())
    }
}

fn check_auth(req: Request<()>) -> Result<Request<()>, Status> {
    let token: MetadataValue<_> = "Bearer some-secret-token".parse().unwrap();

    match req.metadata().get("authorization") {
        Some(t) if token == t => Ok(req),
        a => Err(Status::unauthenticated(format!(
            "No valid auth token {:?} {:?}",
            a, token
        ))),
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
    use super::check_auth;
    use crate::pb::{hsmq_server::Hsmq, Message};
    use crate::server::{HsmqServer, Subscription};

    const TOKEN: &str = "Bearer some-secret-token";

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
    async fn check_auth_deny() {
        let req = tonic::Request::new(());
        let result = check_auth(req);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn check_auth_allow() {
        let token: tonic::metadata::MetadataValue<_> = TOKEN.parse().unwrap();
        let mut req = tonic::Request::new(());
        req.metadata_mut().insert("authorization", token.clone());
        let result = check_auth(req);
        assert!(result.is_ok());
    }
}
