//! 多元索引模块

mod aggregation;
mod create_search_index;
mod delete_search_index;
mod describe_search_index;
mod filter;
mod group_by;
mod list_search_index;
mod query;
mod search_index;
mod sort_by;
mod update_search_index;

use std::{fmt::Display, ops::Range};

pub use aggregation::*;
pub use create_search_index::*;
pub use delete_search_index::*;
pub use describe_search_index::*;
pub use filter::*;
pub use group_by::*;
pub use list_search_index::*;
pub use query::*;
use regex::Regex;
pub use search_index::*;
pub use sort_by::*;
pub use update_search_index::*;

use crate::protos::search::DateTimeUnit;

/// 验证分组名称是否符合规范
///
/// 分组名称应符合以下规范：
///
/// - 由英文字母、数字或下划线组成
/// - 大小写敏感
/// - 长度为 1~128 个字符
/// - 首字母必须为英文字母或下划线
///
/// # Arguments
///
/// * `name` - 要验证的索引名称
///
/// # Returns
/// * `true` - 名称符合规范
/// * `false` - 名称不符合规范
pub(crate) fn validate_group_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 128 {
        return false;
    }

    let first_char = name.chars().next().unwrap();
    if !first_char.is_ascii_alphabetic() && first_char != '_' {
        return false;
    }

    name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// 验证聚合名称
pub(crate) fn validate_aggregation_name(name: &str) -> bool {
    validate_group_name(name)
}

/// 验证是否是符合 OTS 要求的时区字符串
pub(crate) fn validate_timezone_string(tz: &str) -> bool {
    let regex = Regex::new(r"(?m)^[+-]\d{2}:\d{2}$").unwrap();
    regex.is_match(tz)
}

/// 表示时间间隔的枚举类型，用于在多元索引统计聚合中表示日期直方图统计。
/// 标准库和 chrono 库提供的 Duration 和 API 的 [`DateTimeValue`](crate::protos::search::DateTimeValue) 不能完全对应，
/// 所以需要单独定义一个
#[derive(Debug, Copy, Clone)]
pub enum Duration {
    Year(i32),
    Quarter(i32),
    Month(i32),
    Week(i32),
    Day(i32),
    Hour(i32),
    Minute(i32),
    Second(i32),
    Millisecond(i32),
}

impl From<Duration> for crate::protos::search::DateTimeValue {
    fn from(duration: Duration) -> Self {
        match duration {
            Duration::Year(value) => Self {
                value: Some(value),
                unit: Some(DateTimeUnit::Year as i32),
            },

            Duration::Quarter(value) => Self {
                value: Some(value),
                unit: Some(DateTimeUnit::QuarterYear as i32),
            },

            Duration::Month(value) => Self {
                value: Some(value),
                unit: Some(DateTimeUnit::Month as i32),
            },

            Duration::Week(value) => Self {
                value: Some(value),
                unit: Some(DateTimeUnit::Week as i32),
            },

            Duration::Day(value) => Self {
                value: Some(value),
                unit: Some(DateTimeUnit::Day as i32),
            },

            Duration::Hour(value) => Self {
                value: Some(value),
                unit: Some(DateTimeUnit::Hour as i32),
            },

            Duration::Minute(value) => Self {
                value: Some(value),
                unit: Some(DateTimeUnit::Minute as i32),
            },

            Duration::Second(value) => Self {
                value: Some(value),
                unit: Some(DateTimeUnit::Second as i32),
            },

            Duration::Millisecond(value) => Self {
                value: Some(value),
                unit: Some(DateTimeUnit::Millisecond as i32),
            },
        }
    }
}

/// 坐标点，是一个经纬度值。
#[derive(Debug, Default, Clone, Copy)]
pub struct GeoPoint {
    /// 纬度
    pub latitude: f64,

    /// 经度
    pub longitude: f64,
}

impl GeoPoint {
    pub fn new(lat: f64, lng: f64) -> Self {
        Self { latitude: lat, longitude: lng }
    }
}

impl Display for GeoPoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{},{}", self.latitude, self.longitude)
    }
}

impl From<GeoPoint> for crate::protos::search::GeoPoint {
    fn from(value: GeoPoint) -> Self {
        Self {
            lat: Some(value.latitude),
            lon: Some(value.longitude),
        }
    }
}

impl From<Range<f64>> for crate::protos::search::Range {
    fn from(value: Range<f64>) -> Self {
        Self {
            from: Some(value.start),
            to: Some(value.end),
        }
    }
}

#[cfg(test)]
mod test_search_index {
    use crate::{
        OtsClient,
        protos::search::{ColumnReturnType, CreateSearchIndexRequest, FieldSchema, FieldType, IndexSchema, SortOrder},
        search::{
            Aggregation, AvgAggregation, CountAggregation, DistinctCountAggregation, GroupBy, GroupByField, MaxAggregation, MinAggregation,
            PercentilesAggregation, Sorter, SumAggregation, TopRowsAggregation,
        },
        test_util::setup,
    };

    use super::{MatchQuery, Query, SearchQuery, SearchRequest};

    #[tokio::test]
    async fn test_list_search_index() {
        setup();

        let client = OtsClient::from_env();
        let res = client.list_search_index(None).send().await;
        log::debug!("{:#?}", res);
    }

    #[tokio::test]
    async fn test_create_search_index() {
        setup();

        let client = OtsClient::from_env();
        let res = client
            .create_search_index(CreateSearchIndexRequest {
                table_name: "data_types".to_string(),
                index_name: "si_1".to_string(),
                schema: Some(IndexSchema {
                    field_schemas: vec![FieldSchema {
                        field_name: Some("str_col".to_string()),
                        field_type: Some(FieldType::Text as i32),
                        ..Default::default()
                    }],
                    index_setting: None,
                    index_sort: None,
                }),
                ..Default::default()
            })
            .send()
            .await;

        log::debug!("{:#?}", res);

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_describe_search_index() {
        setup();

        let client = OtsClient::from_env();
        let res = client.describe_search_index("data_types", "si_1").send().await;
        log::debug!("{:#?}", res);
    }

    #[tokio::test]
    async fn test_delete_search_index() {
        setup();

        let client = OtsClient::from_env();
        let res = client.delete_search_index("data_types", "si_1").send().await;
        log::debug!("{:#?}", res);
    }

    async fn test_search_match_query_impl() {
        setup();

        let client = OtsClient::from_env();

        let match_query = MatchQuery::new("full_name", "万宇驰");

        let query = Query::Match(match_query);

        let mut search_query = SearchQuery::new(query).sorter(Sorter::PrimaryKey(SortOrder::Asc));

        let mut search_req = SearchRequest::new("users", "users_index", search_query.clone()).column_return_type(ColumnReturnType::ReturnAll);

        let mut total_row = 0;

        loop {
            let res = client.search(search_req.clone()).send().await;

            assert!(res.is_ok());

            let res = res.unwrap();

            // log::debug!("{:#?}", res);

            for row in &res.rows {
                log::debug!(
                    "user id: {:?}, phone number: {:?}",
                    row.get_primary_key_value("user_id"),
                    row.get_column_value("phone_number")
                );
            }

            total_row += res.rows.len();

            if let Some(token) = res.next_token {
                search_query = search_query.token(token);
                search_req = search_req.search_query(search_query.clone());
            } else {
                break;
            }
        }

        log::debug!("total rows: {}", total_row);
    }

    #[tokio::test]
    async fn test_search_match_query() {
        test_search_match_query_impl().await;
    }

    async fn test_search_match_query_with_aggr_impl() {
        setup();

        let client = OtsClient::from_env();

        let match_query = MatchQuery::new("full_name", "万宇驰");

        let query = Query::Match(match_query);
        let group = GroupBy::Field(
            // GroupByField::new("group_by_gender", "gender", 10)
            GroupByField::new("group_by_score", "score", 10),
        );

        let search_query = SearchQuery::new(query)
            .sorter(Sorter::PrimaryKey(SortOrder::Asc))
            .group_by(group)
            .aggregation(Aggregation::Avg(AvgAggregation::new("avg_score", "score")))
            .aggregation(Aggregation::Min(MinAggregation::new("min_score", "score")))
            .aggregation(Aggregation::Max(MaxAggregation::new("max_score", "score")))
            .aggregation(Aggregation::Sum(SumAggregation::new("sum_score", "score")))
            .aggregation(Aggregation::Count(CountAggregation::new("count_score", "score")))
            .aggregation(Aggregation::DistinctCount(DistinctCountAggregation::new("distinct_count_score", "score")))
            .aggregation(Aggregation::TopRows(TopRowsAggregation::new("top_score", 10)))
            .aggregation(Aggregation::Percentiles(PercentilesAggregation::new(
                "percentiles_score",
                "score",
                [25.0f64, 50.0, 75.0, 100.0],
            )));

        let search_req = SearchRequest::new("users", "users_index", search_query).column_return_type(ColumnReturnType::ReturnAll);

        let res = client.search(search_req.clone()).send().await;
        log::debug!("{:?}", res);
    }

    #[tokio::test]
    async fn test_search_match_query_with_aggr() {
        test_search_match_query_with_aggr_impl().await;
    }
}
