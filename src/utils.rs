use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn time_since_epoch() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
}

#[macro_export]
macro_rules! uuid_with_ident {
    ($name:ident) => {
        let mut __uuid_encode_buf = uuid::Uuid::encode_buffer();
        let $name = uuid::Uuid::new_v4();
        let $name = $name.simple().encode_lower(&mut __uuid_encode_buf);
    };
}
