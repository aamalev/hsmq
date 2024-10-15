use lazy_static::lazy_static;
use prometheus::{register_counter_vec, register_gauge_vec, CounterVec, GaugeVec};

lazy_static! {
    pub static ref QUEUE_COUNTER: CounterVec =
        register_counter_vec!("hsmq_queue_total", "Queue events", &["name", "event"]).unwrap();
    pub static ref QUEUE_GAUGE: GaugeVec =
        register_gauge_vec!("hsmq_queue", "Queue metrics", &["name", "m"]).unwrap();
    pub static ref GRPC_COUNTER: CounterVec =
        register_counter_vec!("hsmq_grpc_total", "gRPC events", &["event", "reason"]).unwrap();
    pub static ref GRPC_GAUGE: GaugeVec =
        register_gauge_vec!("hsmq_grpc", "gRPC metrics", &["m"]).unwrap();
    pub static ref JWT_COUNTER: CounterVec =
        register_counter_vec!("hsmq_jwt_total", "JWT auth", &["name"]).unwrap();
    pub static ref REDIS_COUNTER: CounterVec =
        register_counter_vec!("hsmq_redis_total", "Redis events", &["name", "event"]).unwrap();
    pub static ref REDIS_GAUGE: GaugeVec =
        register_gauge_vec!("hsmq_redis", "Redis metrics", &["name", "m"]).unwrap();
}
