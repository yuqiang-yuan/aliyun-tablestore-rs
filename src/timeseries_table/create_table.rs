/// 创建时序表
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/createtimeseriestable>
#[derive(Debug, Clone)]
pub struct CreateTimeseriesTableRequest {
    /// 表名
    pub table_name: String,

    /// 数据生命周期，单位为秒
    pub ttl_seconds: Option<i32>,

    /// 是否允许更新时间线属性列
    pub allow_update_attributes: Option<bool>,

    /// 时间线生命周期，单位为秒。取值必须大于等于 `604800` 秒（即 7 天）或者必须为 `-1`（数据永不过期）。
    pub meta_ttl_seconds: Option<i32>,
}
