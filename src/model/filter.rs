use crate::protos::table_store_filter::{ComparatorType, ValueTransferRule};

use super::Column;

/// 单条件过滤器
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/singlecolumnvaluefilter>
pub struct SingleColumnValueFilter {
    pub comparator: ComparatorType,
    pub column: Column,

    /// 当某行的该列不存在时，设置条件是否过滤。
    pub filter_if_missing: bool,

    /// 是否只对最新版本有效。默认为 `true`
    pub latest_version_only: bool,

    /// 使用正则表达式匹配到字符串后，将字符串转换为 `String` 、 `Integer` 或者 `Double` 类型。
    /// 当某些列中存储了自定义格式数据（例如JSON格式字符串）时，如果用户希望通过某个子字段值来过滤查询该列数据，则需要设置此参数。
    ///
    /// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/valuetransferrule>
    pub value_transfer_rule: Option<ValueTransferRule>
}

/// 将自定义的 SingleColumnFilter 转换成 protobuf 的 SingleColumnFilter
impl From<SingleColumnValueFilter> for crate::protos::table_store_filter::SingleColumnValueFilter {
    fn from(value: SingleColumnValueFilter) -> Self {
        let SingleColumnValueFilter {
            comparator,
            column,
            filter_if_missing,
            latest_version_only,
            value_transfer_rule,
        } = value;

        let Column {
            name,
            value,
            timestamp: _
        } = column;



        crate::protos::table_store_filter::SingleColumnValueFilter {
            comparator: comparator as i32,
            column_name: name,
            column_value: vec![],
            filter_if_missing,
            latest_version_only,
            value_trans_rule: value_transfer_rule
        }
    }
}

/// 过滤器枚举
pub enum Filter {

}
