//! 时序数据

mod get_data;
mod put_data;

pub use get_data::*;
pub use put_data::*;

#[cfg(test)]
mod test_timeseries_data {
    use crate::{
        OtsClient,
        test_util::setup,
        timeseries_model::{TimeseriesKey, TimeseriesRow, TimeseriesVersion},
        util::current_time_ms,
    };

    use super::{GetTimeseriesDataRequest, PutTimeseriesDataRequest};

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
        .end_time_us(1744119422199000)
        .limit(10);

        let resp = client.get_timeseries_data(request).send().await;
        log::debug!("{:?}", resp);
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

    async fn test_put_timeseries_data_impl() {
        setup();

        let client = OtsClient::from_env();

        let ts_us = (current_time_ms() * 1000) as u64;

        let request = PutTimeseriesDataRequest::new("timeseries_demo_with_data")
            .row(
                TimeseriesRow::new()
                    .measurement_name("measure_11")
                    .datasource("data_11")
                    .tag("cluster", "cluster_11")
                    .tag("region", "region_11")
                    .timestamp_us(ts_us)
                    .field_integer("temp", 123),
            )
            .row(
                TimeseriesRow::new()
                    .measurement_name("measure_11")
                    .datasource("data_11")
                    .tag("cluster", "cluster_11")
                    .tag("region", "region_11")
                    .timestamp_us(ts_us + 1000)
                    .field_double("temp", 543.21),
            )
            .supported_table_version(TimeseriesVersion::V1);

        let resp = client.put_timeseries_data(request).send().await;

        log::debug!("{:?}", resp);
    }

    #[tokio::test]
    async fn test_put_timeseries_data() {
        test_put_timeseries_data_impl().await
    }
}
