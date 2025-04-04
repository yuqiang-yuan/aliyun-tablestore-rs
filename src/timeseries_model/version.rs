
/// SDK支持的时序表模型版本号
#[derive(Debug, Copy, Clone, Default)]
pub enum TimeseriesVersion {
    /// 不支持包含自定义时间线标识或作为主键的数据字段的时序表
    #[default]
    V0 = 0,

    /// 支持包含自定义时间线标识和作为主键的数据字段的时序表
    V1 = 1
}
