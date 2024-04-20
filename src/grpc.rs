use crate::metrics::GRPC_COUNTER;
use crate::pb::{
    self, hsmq_server, publish_response, subscription_response, Message, PublishResponse,
    SubscribeQueueRequest, SubscriptionResponse,
};
use crate::server::{self, Consumer, Envelop, HsmqServer, QueueCommand};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::pin::Pin;
use std::{error::Error, io::ErrorKind};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tokio_util::task::task_tracker::TaskTracker;
use tonic::{metadata::MetadataValue, transport::Server as TonicServer, Request, Response, Status};

type HsmqResult<T> = Result<Response<T>, Status>;

pub struct ServiceV1 {
    addr: SocketAddr,
    task_tracker: TaskTracker,
}

impl ServiceV1 {
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
                        GRPC_COUNTER.with_label_values(&["publish", "ok"]).inc();
                        count += 1;
                    } else {
                        GRPC_COUNTER.with_label_values(&["publish", "error"]).inc();
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
        let (server_tx, mut server_rx) = mpsc::channel::<server::Response>(1);

        log::info!(
            "Subscribe queue {:?} {:?}",
            req.remote_addr(),
            req.local_addr()
        );
        let sqr = req.into_inner();
        let consumer = Consumer::new(server_tx);

        let gconsumer = consumer.clone();
        tokio::spawn(async move {
            let mut queues = HashSet::new();
            while let Some(resp) = server_rx.recv().await {
                match resp {
                    server::Response::Message(qmsg) => {
                        let msg: Message = qmsg.message_clone();
                        let resp = SubscriptionResponse {
                            kind: Some(subscription_response::Kind::Message(msg)),
                        };
                        match response_tx.send(Result::<_, Status>::Ok(resp)).await {
                            Ok(_) => {
                                GRPC_COUNTER.with_label_values(&["consume", "ok"]).inc();
                            }
                            Err(_item) => {
                                qmsg.requeue().await;
                                break;
                            }
                        }
                    }
                    server::Response::StartConsume(q) => {
                        queues.insert(q);
                    }
                    server::Response::GracefulShutdown(q) => {
                        queues.remove(&q);
                        if queues.is_empty() {
                            drop(response_tx);
                            break;
                        }
                    }
                }
            }
            log::info!("Subscriber disconnect");
            while let Some(resp) = server_rx.recv().await {
                match resp {
                    server::Response::Message(qmsg) => {
                        qmsg.stop_consume(gconsumer.clone()).await;
                        qmsg.requeue().await;
                    }
                    server::Response::StartConsume(q) => {
                        queues.insert(q);
                    }
                    server::Response::GracefulShutdown(q) => {
                        queues.remove(&q);
                        if queues.is_empty() {
                            break;
                        }
                    }
                }
            }
        });
        for queue_name in sqr.queue.iter() {
            if let Some(queue) = self.queues.get(queue_name) {
                let qtx = queue.tx.clone();
                let cmd = QueueCommand::ConsumeStart(consumer.clone());
                match qtx.send(cmd).await {
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("Consume start error {:?}", e);
                    }
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
        let (response_tx, response_rx) = mpsc::channel(1);
        let mut in_stream = request.into_inner();
        let (server_tx, mut server_rx) = mpsc::channel::<server::Response>(1);

        let consumer = Consumer::new(server_tx);

        while let Some(result) = in_stream.next().await {
            match result {
                Ok(request) => match request.kind {
                    Some(pb::request::Kind::SubscribeQueue(pb::SubscribeQueue {
                        queue,
                        ack: _,
                    })) => {
                        for queue_name in queue.iter() {
                            if let Some(queue) = self.queues.get(queue_name) {
                                let qtx = queue.tx.clone();
                                let cmd = QueueCommand::ConsumeStart(consumer.clone());
                                qtx.send(cmd).await.unwrap();
                            }
                        }
                    }
                    Some(pb::request::Kind::MessageAck(pb::MessageAck { msg_id: _ })) => {}
                    _ => (),
                },
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

        let gconsumer = consumer.clone();
        tokio::spawn(async move {
            let mut queues = HashSet::new();
            while let Some(resp) = server_rx.recv().await {
                match resp {
                    server::Response::Message(qmsg) => {
                        let msg: Message = qmsg.message_clone();
                        let resp = pb::Response {
                            kind: Some(pb::response::Kind::Message(msg)),
                        };
                        match response_tx.send(Result::<_, Status>::Ok(resp)).await {
                            Ok(_) => {
                                GRPC_COUNTER.with_label_values(&["consume", "ok"]).inc();
                            }
                            Err(_item) => {
                                qmsg.requeue().await;
                                break;
                            }
                        }
                    }
                    server::Response::StartConsume(q) => {
                        queues.insert(q);
                    }
                    server::Response::GracefulShutdown(q) => {
                        queues.remove(&q);
                        if queues.is_empty() {
                            drop(response_tx);
                            break;
                        }
                    }
                }
            }
            log::info!("Subscriber disconnect");
            while let Some(resp) = server_rx.recv().await {
                match resp {
                    server::Response::Message(qmsg) => {
                        qmsg.stop_consume(gconsumer.clone()).await;
                        qmsg.requeue().await;
                    }
                    server::Response::StartConsume(q) => {
                        queues.insert(q);
                    }
                    server::Response::GracefulShutdown(q) => {
                        queues.remove(&q);
                        if queues.is_empty() {
                            break;
                        }
                    }
                }
            }
        });

        let output_stream = ReceiverStream::new(response_rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::StreamingStream
        ))
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
        let token: tonic::metadata::MetadataValue<_> = "Bearer some-secret-token".parse().unwrap();
        let mut req = tonic::Request::new(());
        req.metadata_mut().insert("authorization", token.clone());
        let result = check_auth(req);
        assert!(result.is_ok());
    }
}
