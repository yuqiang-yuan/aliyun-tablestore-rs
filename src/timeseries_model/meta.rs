
use super::TimeseriesKey;


#[derive(Debug, Default, Clone)]
pub struct TimeseriesMeta {
    pub key: TimeseriesKey,
    pub attributes: Option<String>,
    pub update_time: Option<u64>,
}

impl From<crate::protos::timeseries::TimeseriesMeta> for TimeseriesMeta {
    fn from(value: crate::protos::timeseries::TimeseriesMeta) -> Self {
        let crate::protos::timeseries::TimeseriesMeta {
            time_series_key,
            attributes,
            update_time,
        } = value;

        Self {
            key: TimeseriesKey::from(time_series_key),
            attributes,
            update_time: update_time.map(|n| n as u64)
        }
    }
}
