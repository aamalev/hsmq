use std::collections::VecDeque;

use prometheus::core::AtomicF64;
use prometheus::core::GenericGauge;

#[derive(Debug)]
pub struct Deque<T> {
    v: tokio::sync::Mutex<VecDeque<T>>,
    metric: GenericGauge<AtomicF64>,
}

impl<T> Deque<T> {
    pub fn new(metric: GenericGauge<AtomicF64>) -> Self {
        let v = tokio::sync::Mutex::new(VecDeque::new());
        Self { metric, v }
    }

    pub fn len(&self) -> usize {
        self.metric.get() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.metric.get() == 0.0
    }

    pub async fn push_front(&self, value: T) {
        self.metric.inc();
        let mut d = self.v.lock().await;
        d.push_front(value);
    }

    pub async fn push_back(&self, value: T) {
        self.metric.inc();
        let mut d = self.v.lock().await;
        d.push_back(value);
    }

    pub async fn pop_front(&self) -> Option<T> {
        let mut d = self.v.lock().await;
        d.pop_front().inspect(|_| {
            self.metric.dec();
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::deque::Deque;
    use crate::metrics;

    #[tokio::test]
    async fn push_front() {
        let deque = Deque::new(metrics::QUEUE_GAUGE.with_label_values(&["deque", "push_front"]));
        deque.push_front(1).await;
        assert_eq!(deque.len(), 1);
    }

    #[tokio::test]
    async fn push_back() {
        let deque = Deque::new(metrics::QUEUE_GAUGE.with_label_values(&["deque", "push_back"]));
        deque.push_back(1).await;
        assert_eq!(deque.len(), 1);
    }

    #[tokio::test]
    async fn pop_front() {
        let deque = Deque::new(metrics::QUEUE_GAUGE.with_label_values(&["deque", "pop_front"]));
        deque.push_front(1).await;
        assert_eq!(deque.len(), 1);
        assert_eq!(deque.pop_front().await, Some(1));
        assert_eq!(deque.len(), 0);
        assert_eq!(deque.pop_front().await, None);
        assert_eq!(deque.len(), 0);
    }
}
