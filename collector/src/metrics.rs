use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::{Duration, sleep};

#[derive(Clone)]
pub struct ThroughputMetrics {
    pub recv: Arc<AtomicU64>,
    pub pubed: Arc<AtomicU64>,
}

impl ThroughputMetrics {
    pub fn new() -> Self {
        Self {
            recv: Arc::new(AtomicU64::new(0)),
            pubed: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn incr_recv(&self) {
        self.recv.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_pub(&self) {
        self.pubed.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn start_logger(self, exchange: &'static str, stream: impl Into<String>) {
        let stream = stream.into();
        let mut last_recv = 0;
        let mut last_pub = 0;

        loop {
            sleep(Duration::from_secs(1)).await;
            let recv_now = self.recv.load(Ordering::Relaxed);
            let pub_now = self.pubed.load(Ordering::Relaxed);
            let recv_rate = recv_now - last_recv;
            let pub_rate = pub_now - last_pub;

            println!(
                "📊 [{}:{}] recv/s={} | pub/s={} | total_recv={} | total_pub={}",
                exchange, stream, recv_rate, pub_rate, recv_now, pub_now
            );

            last_recv = recv_now;
            last_pub = pub_now;
        }
    }
}
