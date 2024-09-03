use crate::pb::Message;
use std::str::FromStr;
pub const CREATED_AT: &str = "created-at";

pub fn repr(m: &Message) -> String {
    let any = m.data.clone().unwrap_or_default();
    let mut result = format!("Message(topic='{}'", m.topic);

    if !m.key.is_empty() {
        result += format!(", key='{}'", &m.key).as_str();
    }

    match any.type_url.as_str() {
        "string" | "int64" => {
            let s = String::from_utf8_lossy(&any.value);
            result += format!(", data='{}'", s).as_str();
        }
        _ => (),
    };

    if let Some(created) = m.headers.get(CREATED_AT) {
        let created = f64::from_str(created).unwrap();
        let now = current_time().as_secs_f64();
        let delta = now - created;
        result += format!(", delta_µs={:.3}", delta * 1_000_000.0).as_str();
    }

    if !m.headers.is_empty() {
        let mut h = format!(", headers={:?}", m.headers);
        h = h.replace('"', "'");
        result += h.as_str();
    };

    result += ")";
    result
}

pub fn current_time() -> std::time::Duration {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
}

#[cfg(test)]
mod tests {
    use crate::{pb::Message, utils::repr};

    #[tokio::test]
    async fn repr_test() {
        let msg = Message::default();
        let r = repr(&msg);
        assert!(!r.is_empty());
    }

    #[tokio::test]
    async fn current_time_test() {
        let ts = super::current_time();
        assert!(!ts.is_zero());
    }
}
