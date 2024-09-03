use hsmq::client_factory::ClientFactory;
use hsmq::config::Config;
use hsmq::launcher;
use hsmq::utils;
use std::path::PathBuf;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::Streaming;

pub fn get_resource(rel_path: &str) -> PathBuf {
    let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("tests/resources");
    d.push(rel_path);
    d
}

async fn build_server(config: Config) {
    let listener = tokio::net::TcpListener::bind(config.node.grpc_address.unwrap())
        .await
        .unwrap();
    tokio::spawn(async move { launcher::run(config, Some(listener)).await.unwrap() });
}

#[tokio::test]
async fn test_streaming() {
    let config = Config::from_file(get_resource("hsmq.toml").as_path()).unwrap();
    build_server(config.clone()).await;
    let topic = "a.b".to_string();

    async fn publish_messages(
        topic: &String,
        tx: &mpsc::Sender<hsmq::pb::Request>,
        stream: &mut Streaming<hsmq::pb::Response>,
    ) -> Vec<String> {
        let mut ids: Vec<String> = Vec::with_capacity(10);
        for _ in 0..10 {
            let mut msg = hsmq::pb::Message::default();
            let now = utils::current_time().as_secs_f64().to_string();
            let msg_id = uuid::Uuid::now_v7().to_string();

            msg.topic.clone_from(topic);
            msg.headers.insert("uuid".to_string(), msg_id.clone());
            msg.headers.insert("ts".to_string(), now);

            let cmd = hsmq::pb::PublishMessage {
                message: Some(msg),
                qos: 1,
                request_id: msg_id.clone(),
            };
            let kind = Some(hsmq::pb::request::Kind::PublishMessage(cmd));
            let req = hsmq::pb::Request { kind };
            tx.send(req).await.unwrap();

            let result = stream.next().await.unwrap().ok();
            assert!(result.is_some_and(|x| x.kind.clone().is_some_and(|y| {
                match y {
                    hsmq::pb::response::Kind::PubAck(pub_ack) => pub_ack.request_id == msg_id,
                    _ => false,
                }
            })));
            ids.push(msg_id.clone());
        }
        ids
    }

    async fn consume_messages(
        tx: &mpsc::Sender<hsmq::pb::Request>,
        stream: &mut Streaming<hsmq::pb::Response>,
    ) -> Vec<String> {
        let mut ids: Vec<String> = Vec::with_capacity(5);
        for _ in 0..10 {
            let item = stream.next();
            let a = item.await.unwrap();
            match a.clone().unwrap().kind {
                Some(hsmq::pb::response::Kind::Message(msg)) => {
                    ids.push(
                        msg.message
                            .clone()
                            .unwrap()
                            .headers
                            .get("uuid")
                            .expect("no message uuid")
                            .to_string(),
                    );
                    let cmd = hsmq::pb::MessageAck { meta: msg.meta };
                    let kind = Some(hsmq::pb::request::Kind::MessageAck(cmd));
                    let req = hsmq::pb::Request { kind };
                    tx.send(req).await.unwrap();
                }
                _ => (),
            }
        }
        ids
    }

    let client_factory = ClientFactory::new(config);
    let mut client = client_factory.create_client().await.unwrap();
    let (tx, rx) = mpsc::channel(1);
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let mut stream = client.streaming(stream).await.unwrap().into_inner();

    let published_message_ids = publish_messages(&topic, &tx, &mut stream).await;

    let cmd = hsmq::pb::SubscribeQueue {
        queues: vec!["a".to_string()],
        prefetch_count: 1,
    };
    let kind = Some(hsmq::pb::request::Kind::SubscribeQueue(cmd));
    let req = hsmq::pb::Request { kind };
    tx.send(req).await.unwrap();
    let consumed_message_ids = consume_messages(&tx, &mut stream).await;

    assert_eq!(consumed_message_ids.len(), 5);
    assert_eq!(published_message_ids[0..5].to_vec(), consumed_message_ids);
}
