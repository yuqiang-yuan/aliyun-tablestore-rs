
use crate::protos::search::DateTimeUnit;

use super::{Aggregation, Query, Sorter};


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

/// 多元索引数据查询配置
#[derive(Debug, Clone)]
pub struct SearchQuery {
    /// 查询条件。
    pub query: Query,

    /// 本次查询的开始位置。
    pub offset: Option<u32>,

    /// 本次查询需要返回的最大数量。
    pub limit: Option<u32>,

    /// 按照指定列对返回结果进行去重。
    ///
    /// 按该列对结果集做折叠，只支持应用于整型、浮点数和 `Keyword` 类型的列，不支持数组类型的列。
    pub collapse_field_name: Option<String>,

    /// 返回结果的排序方式。
    pub sorters: Vec<Sorter>,

    /// 当指定非 PrimaryKeySort 的 sorter 时，默认情况下会主动添加 PrimaryKeySort，
    /// 通过该参数可禁止主动添加PrimaryKeySort
    pub disable_default_pk_sorter: bool,

    /// 是否返回匹配的总行数，默认为 `false`，表示不返回。
    /// 返回匹配的总行数会影响查询性能。
    pub return_total_count: Option<bool>,

    /// 统计聚合配置。
    pub aggregations: Vec<Aggregation>,


}


/// 通过多元索引查询数据。
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/search>
#[derive(Debug, Clone)]
pub struct SearchRequest {
    pub table_name: String,
    pub index_name: String,
    pub search_query: SearchQuery,
}
