use std::io::Cursor;

use prost::Message;

use crate::protos::table_store_filter::{ComparatorType, FilterType, ValueTransferRule};

use super::Column;

/// 单条件过滤器
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/singlecolumnvaluefilter>
#[derive(Debug, Clone)]
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
    pub value_transfer_rule: Option<ValueTransferRule>,
}

impl SingleColumnValueFilter {
    pub fn new() -> Self {
        Self {
            comparator: ComparatorType::CtEqual,
            column: Column::null(""),
            filter_if_missing: false,
            latest_version_only: true,
            value_transfer_rule: None,
        }
    }

    /// 等于
    pub fn equal_column(mut self, col: Column) -> Self {
        self.comparator = ComparatorType::CtEqual;
        self.column = col;

        self
    }

    /// 不等于
    pub fn not_equal(mut self, col: Column) -> Self {
        self.comparator = ComparatorType::CtNotEqual;
        self.column = col;

        self
    }

    /// 大于
    pub fn greater_than(mut self, col: Column) -> Self {
        self.comparator = ComparatorType::CtGreaterThan;
        self.column = col;

        self
    }

    /// 大于等于
    pub fn greater_equal(mut self, col: Column) -> Self {
        self.comparator = ComparatorType::CtGreaterEqual;
        self.column = col;

        self
    }

    /// 小于
    pub fn less_than(mut self, col: Column) -> Self {
        self.comparator = ComparatorType::CtLessThan;
        self.column = col;

        self
    }

    /// 小于等于
    pub fn less_equal(mut self, col: Column) -> Self {
        self.comparator = ComparatorType::CtLessEqual;
        self.column = col;

        self
    }

    /// 存在
    pub fn exists(mut self, col: Column) -> Self {
        self.comparator = ComparatorType::CtExist;
        self.column = col;

        self
    }

    /// 不存在
    pub fn not_exists(mut self, col: Column) -> Self {
        self.comparator = ComparatorType::CtNotExist;
        self.column = col;

        self
    }

    /// 设置当某行的该列不存在时，设置条件是否过滤。
    pub fn filter_if_missing(mut self, value: bool) -> Self {
        self.filter_if_missing = value;

        self
    }

    /// 设置是否只对最新版本有效。默认为 `true`
    pub fn latest_version_only(mut self, value: bool) -> Self {
        self.latest_version_only = value;

        self
    }

    /// 设置转换规则
    pub fn value_transfer_rule(mut self, value: ValueTransferRule) -> Self {
        self.value_transfer_rule = Some(value);

        self
    }

    /// Convert to protobuf bytes
    pub fn into_bytes(self) -> Vec<u8> {
        let msg: crate::protos::table_store_filter::SingleColumnValueFilter = self.into();
        msg.encode_to_vec()
    }
}

impl Default for SingleColumnValueFilter {
    fn default() -> Self {
        Self::new()
    }
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
            op: _,
            timestamp: _,
        } = column;

        // 这里写出的数据不包含 CELL_VALUE 前缀的 4 个字节，
        let mut cursor = Cursor::new(vec![0u8; value.compute_size() as usize]);
        value.write_plain_buffer(&mut cursor);
        let filter_bytes = cursor.into_inner()[4..].into();

        crate::protos::table_store_filter::SingleColumnValueFilter {
            comparator: comparator as i32,
            column_name: name,
            column_value: filter_bytes,
            filter_if_missing,
            latest_version_only,
            value_trans_rule: value_transfer_rule,
        }
    }
}

/// 过滤器枚举
#[derive(Debug, Clone)]
pub enum Filter {
    Single(SingleColumnValueFilter),
}

impl From<Filter> for crate::protos::table_store_filter::Filter {
    fn from(value: Filter) -> Self {
        match value {
            Filter::Single(f) => crate::protos::table_store_filter::Filter {
                r#type: FilterType::FtSingleColumnValue as i32,
                filter: f.into_bytes(),
            },
        }
    }
}
