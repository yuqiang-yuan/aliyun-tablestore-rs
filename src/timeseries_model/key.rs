use std::collections::HashMap;

use crate::{error::OtsError, OtsResult};

use super::{rules::{validate_timeseries_datasource, validate_timeseries_measurement, validate_timeseries_tag_name, validate_timeseries_tag_value}, TimeseriesVersion};


/// 时间线标识
#[derive(Debug, Clone, Default)]
pub struct TimeseriesKey {
    /// 度量名称
    pub measurement_name: Option<String>,

    /// 源
    pub datasource: Option<String>,

    /// 标签列表。适用于 `supported_table_version` 为 `1` 的实例
    pub tags: HashMap<String, String>,
}


impl TimeseriesKey {
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置度量名称
    pub fn measurement_name(mut self, measurement: impl Into<String>) -> Self {
        self.measurement_name = Some(measurement.into());
        self
    }

    /// 设置源
    pub fn datasource(mut self, source: impl Into<String>) -> Self {
        self.datasource = Some(source.into());
        self
    }

    /// 设置 `supported_table_version` 为 `0` 的实例的标签
    // pub fn tags_string(mut self, tags_string: impl Into<String>) -> Self {
    //     self.tags_string = Some(tags_string.into());
    //     self
    // }

    /// 增加一个 `supported_table_version` 为 `1` 的实例的标签
    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// 设置一个 `supported_table_version` 为 `1` 的实例的所有标签
    pub fn tags(mut self, tags: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>) -> Self {
        self.tags = tags.into_iter().map(|(k, v)| (k.into(), v.into())).collect();
        self
    }

    pub(crate) fn validate(&self) -> OtsResult<()> {
        if let Some(s) = &self.measurement_name {
            if !validate_timeseries_measurement(s) {
                return Err(OtsError::ValidationFailed(format!("invalid measurement name: {}", s)));
            }
        }

        if let Some(s) = &self.datasource {
            if !validate_timeseries_datasource(s) {
                return Err(OtsError::ValidationFailed(format!("invalid datasource: {}", s)));
            }
        }

        for (k, v) in &self.tags {
            if !validate_timeseries_tag_name(k) {
                return Err(OtsError::ValidationFailed(format!("invalid tag name: {}", k)));
            }
            if !validate_timeseries_tag_value(v) {
                return Err(OtsError::ValidationFailed(format!("invalid tag value: {}", v)));
            }
        }

        Ok(())
    }

    /// 将 `TimeseriesKey` 转换为 `TimeseriesKey` 的 protobuf 表示
    /// 由于不同的库表版本对应的 protobuf 表示不一样，所以需要根据版本号进行转换
    pub fn into_timeseries_key_with_version(self, version: TimeseriesVersion) -> crate::protos::timeseries::TimeseriesKey {
        let TimeseriesKey {
            measurement_name,
            datasource,
            tags,
        } = self;

        let mut ret = crate::protos::timeseries::TimeseriesKey {
            measurement: measurement_name,
            source: datasource,
            ..Default::default()
        };

        if !tags.is_empty() {
            let mut items = tags.into_iter().collect::<Vec<_>>();
            items.sort_by(|a, b| a.0.cmp(&b.0));

            match version {
                TimeseriesVersion::V0 => {
                    let s = items.into_iter().map(|(k, v)| format!("\"{}={}\"", k, v)).collect::<Vec<_>>().join(",");
                    ret.tags = Some(format!("[{}]", s));
                },

                TimeseriesVersion::V1 => {
                    ret.tag_list = items.into_iter().map(|(k, v)| {
                        crate::protos::timeseries::TimeseriesTag {
                            name: k,
                            value: v,
                        }
                    }).collect();
                },
            }
        }

        ret
    }
}

/// 解析 tags 字符串。
/// 例如：从服务器返回的 tags 字符串为： `"[\"cluster=cluster_3\",\"region=region_7\"]"`
pub(crate) fn parse_tags(tags: &str) -> HashMap<String, String> {
    if tags.is_empty() || tags.len() < 2 {
        return HashMap::new();
    }

    let s = &tags[1..tags.len() - 1];

    let mut ret = HashMap::new();

    s.split(",").for_each(|kv| {
        let mut parts = kv.split("=");
        if let (Some(k), Some(v)) = (parts.next(), parts.next()) {
            ret.insert(k.to_string(), v.to_string());
        }
    });

    ret
}
