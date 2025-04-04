//! 时序数据

mod get_data;

pub use get_data::*;

#[cfg(test)]
mod test_timeseries_data {
    use crate::{OtsClient, test_util::setup, timeseries_model::TimeseriesKey};

    use super::GetTimeseriesDataRequest;

    /// Test query timeseries data
    async fn test_get_timeseries_data_impl() {
        setup();
        let client = OtsClient::from_env();

        let request = GetTimeseriesDataRequest::new(
            "timeseries_demo_with_data",
            TimeseriesKey::new()
                .measurement_name("measure_7")
                .datasource("data_3")
                .tag("cluster", "cluster_3")
                .tag("region", "region_7"),
        )
        .end_time_us(1743735588398000)
        .limit(10);

        let resp = client.get_timeseries_data(request).send().await;
        assert!(resp.is_ok());

        let resp = resp.unwrap();
        for row in &resp.rows {
            assert_eq!(&Some("measure_7".to_string()), &row.key.measurement_name);

            for col in &row.fields {
                log::debug!("{}: {} => {:?}", row.timestamp_us, col.name, col.value);
            }
        }

        log::debug!("total rows returned: {}", resp.rows.len());
    }

    #[tokio::test]
    async fn test_get_timeseries_data() {
        test_get_timeseries_data_impl().await;
    }
}
