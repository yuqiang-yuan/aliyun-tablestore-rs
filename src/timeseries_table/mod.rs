//! 时序表模块
//!

mod create_table;
mod describe_table;

pub use create_table::*;
pub use describe_table::*;

#[cfg(test)]
mod test_timeseries_table {
    use crate::{test_util::setup, OtsClient};

    use super::CreateTimeseriesTableRequest;

    async fn test_create_timeseries_table_impl() {
        setup();
        let client = OtsClient::from_env();

        let request = CreateTimeseriesTableRequest::new("my_ts_test");

        let resp = client.create_timeseries_table(request).send().await;

        assert!(resp.is_ok());

        log::debug!("{:?}", resp);
    }

    #[tokio::test]
    async fn test_create_timeseries_table() {
        test_create_timeseries_table_impl().await;
    }
}
