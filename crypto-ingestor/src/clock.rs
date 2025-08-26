use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use crate::metrics::CLOCK_SKEW;

pub static CLOCK_SKEW_MS: Lazy<AtomicI64> = Lazy::new(|| AtomicI64::new(0));

pub fn spawn_clock_sync() {
    tokio::spawn(async {
        loop {
            match ntp::request("time.google.com:123") {
                Ok(resp) => {
                    let ts: time::Timespec = resp.transmit_time.into();
                    let ntp_ms = ts.sec * 1000 + (ts.nsec as i64 / 1_000_000);
                    let now_ms = chrono::Utc::now().timestamp_millis();
                    let offset = now_ms - ntp_ms;
                    CLOCK_SKEW_MS.store(offset, Ordering::Relaxed);
                    CLOCK_SKEW.with_label_values(&["ntp"]).set(offset);
                }
                Err(e) => {
                    tracing::warn!(error=%e, "ntp sync failed");
                }
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    });
}

pub fn current_skew_ms() -> i64 {
    CLOCK_SKEW_MS.load(Ordering::Relaxed)
}

