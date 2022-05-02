use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn time_since_epoch() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
}
