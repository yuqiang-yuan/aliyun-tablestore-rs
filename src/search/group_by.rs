use std::ops::Range;

use prost::Message;

use crate::{
    OtsResult,
    error::OtsError,
    model::ColumnValue,
    protos::search::{FieldRange, GeoHashPrecision, GroupByType, SortOrder},
    table::rules::validate_column_name,
};

use super::{Aggregation, Duration, GeoPoint, Query, validate_aggregation_name, validate_group_name, validate_timezone_string};

/// 分组中的item排序规则集。
#[derive(Debug, Clone)]
pub enum GroupBySorter {
    /// 按照分组对应值排序的排序规则。
    GroupKey(SortOrder),

    /// 按照分组中总行数排序的排序规则
    RowCount(SortOrder),

    /// 按照某个子统计聚合排序的排序规则。元素 `.0` 是子统计聚合的名字
    SubAggregation(String, SortOrder),
}

impl From<GroupBySorter> for crate::protos::search::GroupBySorter {
    fn from(value: GroupBySorter) -> Self {
        let mut ret = crate::protos::search::GroupBySorter {
            group_key_sort: None,
            row_count_sort: None,
            sub_agg_sort: None,
        };

        match value {
            GroupBySorter::GroupKey(order) => {
                ret.group_key_sort = Some(crate::protos::search::GroupKeySort { order: Some(order as i32) });
            }
            GroupBySorter::RowCount(order) => {
                ret.row_count_sort = Some(crate::protos::search::RowCountSort { order: Some(order as i32) });
            }
            GroupBySorter::SubAggregation(name, order) => {
                ret.sub_agg_sort = Some(crate::protos::search::SubAggSort {
                    sub_agg_name: Some(name),
                    order: Some(order as i32),
                });
            }
        }

        ret
    }
}

impl GroupBySorter {
    pub(crate) fn validate(&self) -> OtsResult<()> {
        match self {
            Self::SubAggregation(name, _) => {
                if !validate_aggregation_name(name) {
                    return Err(OtsError::ValidationFailed(format!("invalid aggregation name: {}", name)));
                }

                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// 从一组排序配置生成 GroupBySort
impl<T, S> From<T> for crate::protos::search::GroupBySort
where
    T: IntoIterator<Item = S>,
    S: Into<crate::protos::search::GroupBySorter>,
{
    fn from(value: T) -> Self {
        Self {
            sorters: value.into_iter().map(Into::into).collect(),
        }
    }
}

/// 对某一个字段进行分组统计。
///
/// 举例：库存账单里有“篮球”、“足球”、“羽毛球”等，对这一个字段进行聚合，返回： “篮球：10个”，“足球：5个”，“网球：1个”这样的聚合信息。
#[derive(Debug, Default, Clone)]
pub struct GroupByField {
    /// GroupBy 的名字，之后从 GroupBy 结果列表中根据该名字拿到 GroupBy 结果
    pub name: String,

    /// 字段名字
    pub field_name: String,

    /// 返回的分组数量，默认值为 `10`。最大值为 `2000`。当分组数量超过 `2000` 时，只会返回前 `2000` 个分组。
    pub size: u32,

    /// 分组中的item排序规则，默认按照分组中item的数量降序排序，多个排序则按照添加的顺序进行排列。
    pub sorters: Vec<GroupBySorter>,

    /// 子统计聚合Aggregation，子统计聚合会根据分组内容再进行一次统计聚合分析。
    pub sub_aggregations: Vec<Aggregation>,

    /// 子统计聚合GroupBy，子统计聚合会根据分组内容再进行一次统计聚合分析。
    pub sub_group_bys: Vec<GroupBy>,

    /// 最小行数。当分组中的行数小于最小行数时，不会返回此分组的统计结果。
    pub min_doc_count: Option<u64>,
}

impl GroupByField {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            ..Default::default()
        }
    }

    /// 设置名称
    pub fn name(mut self, name: &str) -> Self {
        self.name = name.to_string();

        self
    }

    /// 设置字段名
    pub fn field_name(mut self, field_name: &str) -> Self {
        self.field_name = field_name.to_string();

        self
    }

    /// 设置分组数量
    pub fn size(mut self, size: u32) -> Self {
        self.size = size;

        self
    }

    /// 设置最小行数
    pub fn min_doc_count(mut self, min_doc_count: u64) -> Self {
        self.min_doc_count = Some(min_doc_count);

        self
    }

    /// 增加排序配置
    pub fn sorter(mut self, sorter: GroupBySorter) -> Self {
        self.sorters.push(sorter);

        self
    }

    /// 设置排序配置
    pub fn sorters(mut self, sorters: impl IntoIterator<Item = GroupBySorter>) -> Self {
        self.sorters = sorters.into_iter().collect();

        self
    }

    /// 增加子聚合
    pub fn sub_aggregation(mut self, aggr: Aggregation) -> Self {
        self.sub_aggregations.push(aggr);

        self
    }

    /// 设置子聚合
    pub fn sub_aggregations(mut self, aggregations: impl IntoIterator<Item = Aggregation>) -> Self {
        self.sub_aggregations = aggregations.into_iter().collect();

        self
    }

    /// 增加子分组
    pub fn sub_group_by(mut self, sub_group_by: GroupBy) -> Self {
        self.sub_group_bys.push(sub_group_by);

        self
    }

    /// 设置子分组
    pub fn sub_group_bys(mut self, sub_group_bys: impl IntoIterator<Item = GroupBy>) -> Self {
        self.sub_group_bys = sub_group_bys.into_iter().collect();

        self
    }

    /// 验证数据
    pub(crate) fn validate(&self) -> OtsResult<()> {
        if !validate_group_name(&self.name) {
            return Err(OtsError::ValidationFailed(format!("invalid group name: {}", self.name)));
        }

        if !validate_column_name(&self.field_name) {
            return Err(OtsError::ValidationFailed(format!("invalid field name: {}", self.field_name)));
        }

        if self.size == 0 {
            return Err(OtsError::ValidationFailed("invalid size: 0".to_string()));
        }

        if self.size > i32::MAX as u32 {
            return Err(OtsError::ValidationFailed(format!("size is too large: {}", self.size)));
        }

        for s in &self.sorters {
            s.validate()?;
        }

        for g in &self.sub_group_bys {
            g.validate()?;
        }

        for a in &self.sub_aggregations {
            a.validate()?;
        }

        Ok(())
    }
}

impl From<GroupByField> for crate::protos::search::GroupByField {
    fn from(value: GroupByField) -> Self {
        let GroupByField {
            name: _,
            field_name,
            size,
            sorters,
            sub_aggregations,
            sub_group_bys,
            min_doc_count,
        } = value;

        Self {
            field_name: Some(field_name),
            size: Some(size as i32),
            sort: Some(crate::protos::search::GroupBySort::from(sorters)),
            sub_aggs: Some(crate::protos::search::Aggregations::from(sub_aggregations)),
            sub_group_bys: Some(crate::protos::search::GroupBys::from(sub_group_bys)),
            min_doc_count: min_doc_count.map(|n| n as i64),
        }
    }
}

/// 在多元索引统计聚合中表示过滤条件分组，用于按照过滤条件对查询结果进行分组，获取每个过滤条件匹配到的数量，返回结果的顺序和添加过滤条件的顺序一致。
#[derive(Debug, Clone, Default)]
pub struct GroupByFilter {
    /// 分组名称
    pub name: String,

    /// 过滤器
    pub filters: Vec<Query>,

    /// 子聚合
    pub sub_aggregations: Vec<Aggregation>,

    /// 子分组
    pub sub_group_bys: Vec<GroupBy>,
}

impl GroupByFilter {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            ..Default::default()
        }
    }

    /// 设置名称
    pub fn name(mut self, name: &str) -> Self {
        self.name = name.to_string();

        self
    }

    /// 增加子聚合
    pub fn sub_aggregation(mut self, aggr: Aggregation) -> Self {
        self.sub_aggregations.push(aggr);

        self
    }

    /// 设置子聚合
    pub fn sub_aggregations(mut self, aggregations: impl IntoIterator<Item = Aggregation>) -> Self {
        self.sub_aggregations = aggregations.into_iter().collect();

        self
    }

    /// 增加子分组
    pub fn sub_group_by(mut self, sub_group_by: GroupBy) -> Self {
        self.sub_group_bys.push(sub_group_by);

        self
    }

    /// 设置子分组
    pub fn sub_group_bys(mut self, sub_group_bys: impl IntoIterator<Item = GroupBy>) -> Self {
        self.sub_group_bys = sub_group_bys.into_iter().collect();

        self
    }

    /// 添加一个过滤器
    pub fn filter(mut self, filter: Query) -> Self {
        self.filters.push(filter);

        self
    }

    /// 设置过滤器
    pub fn filters(mut self, filters: impl IntoIterator<Item = Query>) -> Self {
        self.filters = filters.into_iter().collect();

        self
    }

    /// 验证数据
    pub(crate) fn validate(&self) -> OtsResult<()> {
        if !validate_group_name(&self.name) {
            return Err(OtsError::ValidationFailed(format!("invalid group name: {}", self.name)));
        }

        if self.filters.is_empty() {
            return Err(OtsError::ValidationFailed("filters are required, please set a valid value".to_string()));
        }

        for g in &self.sub_group_bys {
            g.validate()?;
        }

        for a in &self.sub_aggregations {
            a.validate()?;
        }

        Ok(())
    }
}

impl From<GroupByFilter> for crate::protos::search::GroupByFilter {
    fn from(value: GroupByFilter) -> Self {
        let GroupByFilter {
            name: _,
            filters,
            sub_aggregations,
            sub_group_bys,
        } = value;

        Self {
            filters: filters.into_iter().map(crate::protos::search::Query::from).collect(),
            sub_aggs: Some(crate::protos::search::Aggregations::from(sub_aggregations)),
            sub_group_bys: Some(crate::protos::search::GroupBys::from(sub_group_bys)),
        }
    }
}

/// 在多元索引统计聚合中表示直方图统计，用于按照指定数据间隔对查询结果进行分组，字段值在相同范围内放到同一分组内，返回每个分组的值和该值对应的个数。
#[derive(Debug, Clone, Default)]
pub struct GroupByHistogram {
    /// 分组名称
    pub name: String,

    /// 字段名字
    pub field_name: String,

    /// 统计间隔
    pub interval: ColumnValue,

    /// 统计范围，与interval参数配合使用限制分组的数量。
    /// `(max_value - min_value) / interval` 的值不能超过 `2000`
    pub min_value: ColumnValue,

    /// 统计范围，与interval参数配合使用限制分组的数量。
    pub max_value: ColumnValue,

    /// 分组偏差量
    pub offset: Option<ColumnValue>,

    /// 当某行数据中的字段为空时，字段值的默认值
    ///
    /// - 如果未设置 missing 值，则在统计聚合时会忽略该行。
    /// - 如果设置了 missing 值，则使用 missing 值作为字段值的默认值参与统计聚合。
    pub missing_value: Option<ColumnValue>,

    /// 最小行数。当分组中的行数小于最小行数时，不会返回此分组的统计结果。
    pub min_doc_count: Option<u64>,

    /// 分组中的item排序规则，默认按照分组中item的数量降序排序，多个排序则按照添加的顺序进行排列。
    pub sorters: Vec<GroupBySorter>,

    /// 子统计聚合Aggregation，子统计聚合会根据分组内容再进行一次统计聚合分析。
    pub sub_aggregations: Vec<Aggregation>,

    /// 子统计聚合GroupBy，子统计聚合会根据分组内容再进行一次统计聚合分析。
    pub sub_group_bys: Vec<GroupBy>,
}

impl GroupByHistogram {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            ..Default::default()
        }
    }
    /// 设置名称
    pub fn name(mut self, name: &str) -> Self {
        self.name = name.to_string();

        self
    }

    /// 设置字段名
    pub fn field_name(mut self, field_name: &str) -> Self {
        self.field_name = field_name.to_string();

        self
    }

    /// 设置间隔
    pub fn interval(mut self, interval: ColumnValue) -> Self {
        self.interval = interval;

        self
    }

    /// 设置最小值
    pub fn min_value(mut self, min_value: ColumnValue) -> Self {
        self.min_value = min_value;

        self
    }

    /// 设置最大值
    pub fn max_value(mut self, max_value: ColumnValue) -> Self {
        self.max_value = max_value;

        self
    }

    /// 设置最小行数
    pub fn min_doc_count(mut self, min_doc_count: u64) -> Self {
        self.min_doc_count = Some(min_doc_count);

        self
    }

    /// 设置分组偏差量
    pub fn offset(mut self, offset: ColumnValue) -> Self {
        self.offset = Some(offset);

        self
    }

    /// 增加排序配置
    pub fn sorter(mut self, sorter: GroupBySorter) -> Self {
        self.sorters.push(sorter);

        self
    }

    /// 设置排序配置
    pub fn sorters(mut self, sorters: impl IntoIterator<Item = GroupBySorter>) -> Self {
        self.sorters = sorters.into_iter().collect();

        self
    }

    /// 增加子聚合
    pub fn sub_aggregation(mut self, aggr: Aggregation) -> Self {
        self.sub_aggregations.push(aggr);

        self
    }

    /// 设置子聚合
    pub fn sub_aggregations(mut self, aggregations: impl IntoIterator<Item = Aggregation>) -> Self {
        self.sub_aggregations = aggregations.into_iter().collect();

        self
    }

    /// 增加子分组
    pub fn sub_group_by(mut self, sub_group_by: GroupBy) -> Self {
        self.sub_group_bys.push(sub_group_by);

        self
    }

    /// 设置子分组
    pub fn sub_group_bys(mut self, sub_group_bys: impl IntoIterator<Item = GroupBy>) -> Self {
        self.sub_group_bys = sub_group_bys.into_iter().collect();

        self
    }

    /// 验证数据
    pub(crate) fn validate(&self) -> OtsResult<()> {
        if !validate_group_name(&self.name) {
            return Err(OtsError::ValidationFailed(format!("invalid group name: {}", self.name)));
        }

        if !validate_column_name(&self.field_name) {
            return Err(OtsError::ValidationFailed(format!("invalid field name: {}", self.field_name)));
        }

        if self.interval == ColumnValue::Null {
            return Err(OtsError::ValidationFailed("interval is required, please set a valid value".to_string()));
        }

        if self.min_value == ColumnValue::Null {
            return Err(OtsError::ValidationFailed("field_range.min is required, please set a valid value".to_string()));
        }

        if self.max_value == ColumnValue::Null {
            return Err(OtsError::ValidationFailed("field_range.max is required, please set a valid value".to_string()));
        }

        for s in &self.sorters {
            s.validate()?;
        }

        for g in &self.sub_group_bys {
            g.validate()?;
        }

        for a in &self.sub_aggregations {
            a.validate()?;
        }

        Ok(())
    }
}

impl From<GroupByHistogram> for crate::protos::search::GroupByHistogram {
    fn from(value: GroupByHistogram) -> Self {
        let GroupByHistogram {
            name: _,
            field_name,
            interval,
            min_value,
            max_value,
            missing_value,
            min_doc_count,
            sorters,
            sub_aggregations,
            sub_group_bys,
            offset,
        } = value;

        Self {
            field_name: Some(field_name),
            interval: Some(interval.encode_plain_buffer()),
            missing: missing_value.map(|v| v.encode_plain_buffer()),
            min_doc_count: min_doc_count.map(|v| v as i64),
            sort: Some(crate::protos::search::GroupBySort::from(sorters)),
            field_range: Some(crate::protos::search::FieldRange {
                min: Some(min_value.encode_plain_buffer()),
                max: Some(max_value.encode_plain_buffer()),
            }),
            sub_aggs: Some(crate::protos::search::Aggregations::from(sub_aggregations)),
            sub_group_bys: Some(crate::protos::search::GroupBys::from(sub_group_bys)),
            offset: offset.map(|v| v.encode_plain_buffer()),
        }
    }
}

/// 在多元索引统计聚合中表示范围分组，用于根据一个字段的范围对查询结果进行分组，字段值在某范围内放到同一分组内，返回每个范围中相应的item个数。
#[derive(Debug, Clone, Default)]
pub struct GroupByRange {
    // GroupBy 的名字，之后从 GroupBy 结果列表中根据该名字拿到 GroupBy 结果
    pub name: String,

    /// 字段名字
    pub field_name: String,

    /// 分组的范围配置，范围为左闭右开的区间。
    pub ranges: Vec<Range<f64>>,

    /// 子统计聚合Aggregation，子统计聚合会根据分组内容再进行一次统计聚合分析。
    pub sub_aggregations: Vec<Aggregation>,

    /// 子统计聚合GroupBy，子统计聚合会根据分组内容再进行一次统计聚合分析。
    pub sub_group_bys: Vec<GroupBy>,
}

impl GroupByRange {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            ..Default::default()
        }
    }

    /// 设置名称
    pub fn name(mut self, name: &str) -> Self {
        self.name = name.to_string();

        self
    }

    /// 设置字段名
    pub fn field_name(mut self, field_name: &str) -> Self {
        self.field_name = field_name.to_string();

        self
    }

    /// 增加一组范围配置
    pub fn range(mut self, range: Range<f64>) -> Self {
        self.ranges.push(range);

        self
    }

    /// 设置分组范围
    pub fn ranges(mut self, ranges: impl IntoIterator<Item = Range<f64>>) -> Self {
        self.ranges = ranges.into_iter().collect();

        self
    }

    /// 增加子聚合
    pub fn sub_aggregation(mut self, aggr: Aggregation) -> Self {
        self.sub_aggregations.push(aggr);

        self
    }

    /// 设置子聚合
    pub fn sub_aggregations(mut self, aggregations: impl IntoIterator<Item = Aggregation>) -> Self {
        self.sub_aggregations = aggregations.into_iter().collect();

        self
    }

    /// 增加子分组
    pub fn sub_group_by(mut self, sub_group_by: GroupBy) -> Self {
        self.sub_group_bys.push(sub_group_by);

        self
    }

    /// 设置子分组
    pub fn sub_group_bys(mut self, sub_group_bys: impl IntoIterator<Item = GroupBy>) -> Self {
        self.sub_group_bys = sub_group_bys.into_iter().collect();

        self
    }

    /// 验证数据
    pub(crate) fn validate(&self) -> OtsResult<()> {
        if !validate_group_name(&self.name) {
            return Err(OtsError::ValidationFailed(format!("invalid group name: {}", self.name)));
        }

        if !validate_column_name(&self.field_name) {
            return Err(OtsError::ValidationFailed(format!("invalid field name: {}", self.field_name)));
        }

        if self.ranges.is_empty() {
            return Err(OtsError::ValidationFailed("ranges is required, please set a valid value".to_string()));
        }

        for g in &self.sub_group_bys {
            g.validate()?;
        }

        for a in &self.sub_aggregations {
            a.validate()?;
        }

        Ok(())
    }
}

impl From<GroupByRange> for crate::protos::search::GroupByRange {
    fn from(value: GroupByRange) -> Self {
        let GroupByRange {
            name: _,
            field_name,
            ranges,
            sub_aggregations,
            sub_group_bys,
        } = value;

        Self {
            field_name: Some(field_name),
            ranges: ranges.into_iter().map(|r| r.into()).collect(),
            sub_aggs: Some(crate::protos::search::Aggregations::from(sub_aggregations)),
            sub_group_bys: Some(crate::protos::search::GroupBys::from(sub_group_bys)),
        }
    }
}

/// 在多元索引统计聚合中表示日期直方图统计，用于对日期字段类型的数据按照指定间隔对查询结果进行分组，字段值在相同范围内放到同一分组内，返回每个分组的值和该值对应的个数。
#[derive(Debug, Clone, Default)]
pub struct GroupByDateHistogram {
    /// GroupBy 的名字，之后从 GroupBy 结果列表中根据该名字拿到 GroupBy 结果
    pub name: String,

    /// 字段名字
    pub field_name: String,

    /// 统计间隔。**注意**这个是必填属性。为了可以 Default 搞成了这个样子
    pub interval: Option<Duration>,

    /// 分组偏差量
    pub offset: Option<Duration>,

    /// 统计范围，与interval参数配合使用限制分组的数量。
    /// `(max_value - min_value) / interval` 的值不能超过 `2000`
    pub min_value: ColumnValue,

    /// 统计范围，与interval参数配合使用限制分组的数量。
    pub max_value: ColumnValue,

    /// 当某行数据中的字段为空时，字段值的默认值
    ///
    /// - 如果未设置 missing 值，则在统计聚合时会忽略该行。
    /// - 如果设置了 missing 值，则使用 missing 值作为字段值的默认值参与统计聚合。
    pub missing_value: Option<ColumnValue>,

    /// 最小行数。当分组中的行数小于最小行数时，不会返回此分组的统计结果。
    pub min_doc_count: Option<u64>,

    /// 时区。格式为 `+hh:mm` 或者 `-hh:mm` ，例如 `+08:00` 、 `-09:00`。
    pub timezone: Option<String>,

    /// 分组中的item排序规则，默认按照分组中item的数量降序排序，多个排序则按照添加的顺序进行排列。
    pub sorters: Vec<GroupBySorter>,

    /// 子统计聚合Aggregation，子统计聚合会根据分组内容再进行一次统计聚合分析。
    pub sub_aggregations: Vec<Aggregation>,

    /// 子统计聚合GroupBy，子统计聚合会根据分组内容再进行一次统计聚合分析。
    pub sub_group_bys: Vec<GroupBy>,
}

impl GroupByDateHistogram {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            ..Default::default()
        }
    }

    /// 设置名称
    pub fn name(mut self, name: &str) -> Self {
        self.name = name.to_string();

        self
    }

    /// 设置字段名
    pub fn field_name(mut self, field_name: &str) -> Self {
        self.field_name = field_name.to_string();

        self
    }

    /// 设置间隔
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = Some(interval);

        self
    }

    /// 设置最小值
    pub fn min_value(mut self, min_value: ColumnValue) -> Self {
        self.min_value = min_value;

        self
    }

    /// 设置最大值
    pub fn max_value(mut self, max_value: ColumnValue) -> Self {
        self.max_value = max_value;

        self
    }

    /// 设置最小行数
    pub fn min_doc_count(mut self, min_doc_count: u64) -> Self {
        self.min_doc_count = Some(min_doc_count);

        self
    }

    /// 设置分组偏差量
    pub fn offset(mut self, offset: Duration) -> Self {
        self.offset = Some(offset);

        self
    }

    /// 增加排序配置
    pub fn sorter(mut self, sorter: GroupBySorter) -> Self {
        self.sorters.push(sorter);

        self
    }

    /// 设置排序配置
    pub fn sorters(mut self, sorters: impl IntoIterator<Item = GroupBySorter>) -> Self {
        self.sorters = sorters.into_iter().collect();

        self
    }

    /// 增加子聚合
    pub fn sub_aggregation(mut self, aggr: Aggregation) -> Self {
        self.sub_aggregations.push(aggr);

        self
    }

    /// 设置子聚合
    pub fn sub_aggregations(mut self, aggregations: impl IntoIterator<Item = Aggregation>) -> Self {
        self.sub_aggregations = aggregations.into_iter().collect();

        self
    }

    /// 增加子分组
    pub fn sub_group_by(mut self, sub_group_by: GroupBy) -> Self {
        self.sub_group_bys.push(sub_group_by);

        self
    }

    /// 设置子分组
    pub fn sub_group_bys(mut self, sub_group_bys: impl IntoIterator<Item = GroupBy>) -> Self {
        self.sub_group_bys = sub_group_bys.into_iter().collect();

        self
    }

    /// 验证数据
    pub(crate) fn validate(&self) -> OtsResult<()> {
        if !validate_group_name(&self.name) {
            return Err(OtsError::ValidationFailed(format!("invalid group name: {}", self.name)));
        }

        if !validate_column_name(&self.field_name) {
            return Err(OtsError::ValidationFailed(format!("invalid field name: {}", self.field_name)));
        }

        if self.interval.is_none() {
            return Err(OtsError::ValidationFailed("interval is required, please set a valid value".to_string()));
        }

        if self.min_value == ColumnValue::Null {
            return Err(OtsError::ValidationFailed("field_range.min is required, please set a valid value".to_string()));
        }

        if self.max_value == ColumnValue::Null {
            return Err(OtsError::ValidationFailed("field_range.max is required, please set a valid value".to_string()));
        }

        if let Some(n) = self.min_doc_count {
            if n > i64::MAX as u64 {
                return Err(OtsError::ValidationFailed("min_doc_count must be less than or equal to i64::MAX".to_string()));
            }
        }

        if let Some(v) = &self.missing_value {
            if v == &ColumnValue::Null {
                return Err(OtsError::ValidationFailed("Please set a valid missing value".to_string()));
            }
        }

        if let Some(s) = &self.timezone {
            if !validate_timezone_string(s.as_str()) {
                return Err(OtsError::ValidationFailed(format!(
                    "invalid timezone string: {}. It should be like `+08:00` or `-08:00`",
                    s
                )));
            }
        }

        for s in &self.sorters {
            s.validate()?;
        }

        for g in &self.sub_group_bys {
            g.validate()?;
        }

        for a in &self.sub_aggregations {
            a.validate()?;
        }

        Ok(())
    }
}

impl From<GroupByDateHistogram> for crate::protos::search::GroupByDateHistogram {
    fn from(value: GroupByDateHistogram) -> Self {
        let GroupByDateHistogram {
            name: _,
            field_name,
            interval,
            offset,
            min_value,
            max_value,
            missing_value,
            min_doc_count,
            timezone,
            sorters,
            sub_aggregations,
            sub_group_bys,
        } = value;

        Self {
            field_name: Some(field_name),
            interval: interval.map(|i| i.into()),
            offset: offset.map(|i| i.into()),
            field_range: Some(FieldRange {
                min: Some(min_value.encode_plain_buffer()),
                max: Some(max_value.encode_plain_buffer()),
            }),
            missing: missing_value.map(|v| v.encode_plain_buffer()),
            min_doc_count: min_doc_count.map(|v| v as i64),
            time_zone: timezone,
            sort: Some(crate::protos::search::GroupBySort::from(sorters)),
            sub_aggs: Some(crate::protos::search::Aggregations::from(sub_aggregations)),
            sub_group_bys: Some(crate::protos::search::GroupBys::from(sub_group_bys)),
        }
    }
}

/// 对 GeoPoint 类型的字段按照地理区域进行分组统计
#[derive(Debug, Clone, Default)]
pub struct GroupByGeoGrid {
    /// 分组名称
    pub name: String,

    /// 字段名称
    pub field_name: String,

    /// 返回的分组数量
    pub size: u32,

    /// GroupBy 的精度
    pub precision: GeoHashPrecision,

    /// 子统计聚合Aggregation，子统计聚合会根据分组内容再进行一次统计聚合分析。
    pub sub_aggregations: Vec<Aggregation>,

    /// 子统计聚合GroupBy，子统计聚合会根据分组内容再进行一次统计聚合分析。
    pub sub_group_bys: Vec<GroupBy>,
}

impl GroupByGeoGrid {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            ..Default::default()
        }
    }

    /// 设置名称
    pub fn name(mut self, name: &str) -> Self {
        self.name = name.to_string();

        self
    }

    /// 设置字段名
    pub fn field_name(mut self, field_name: &str) -> Self {
        self.field_name = field_name.to_string();

        self
    }

    /// 设置分组数量
    pub fn size(mut self, size: u32) -> Self {
        self.size = size;

        self
    }

    /// 增加子聚合
    pub fn sub_aggregation(mut self, aggr: Aggregation) -> Self {
        self.sub_aggregations.push(aggr);

        self
    }

    /// 设置子聚合
    pub fn sub_aggregations(mut self, aggregations: impl IntoIterator<Item = Aggregation>) -> Self {
        self.sub_aggregations = aggregations.into_iter().collect();

        self
    }

    /// 增加子分组
    pub fn sub_group_by(mut self, sub_group_by: GroupBy) -> Self {
        self.sub_group_bys.push(sub_group_by);

        self
    }

    /// 设置子分组
    pub fn sub_group_bys(mut self, sub_group_bys: impl IntoIterator<Item = GroupBy>) -> Self {
        self.sub_group_bys = sub_group_bys.into_iter().collect();

        self
    }

    pub(crate) fn validate(&self) -> OtsResult<()> {
        if !validate_group_name(&self.name) {
            return Err(OtsError::ValidationFailed(format!("invalid group name: {}", self.name)));
        }

        if !validate_column_name(&self.field_name) {
            return Err(OtsError::ValidationFailed(format!("invalid field name: {}", self.field_name)));
        }

        if self.size > i32::MAX as u32 {
            return Err(OtsError::ValidationFailed("size is too large".to_string()));
        }

        for g in &self.sub_group_bys {
            g.validate()?;
        }

        for a in &self.sub_aggregations {
            a.validate()?;
        }

        Ok(())
    }
}

impl From<GroupByGeoGrid> for crate::protos::search::GroupByGeoGrid {
    fn from(value: GroupByGeoGrid) -> Self {
        let GroupByGeoGrid {
            name: _,
            field_name,
            size,
            precision,
            sub_aggregations,
            sub_group_bys,
        } = value;

        Self {
            field_name: Some(field_name),
            precision: Some(precision as i32),
            size: Some(size as i32),
            sub_aggs: Some(crate::protos::search::Aggregations::from(sub_aggregations)),
            sub_group_bys: Some(crate::protos::search::GroupBys::from(sub_group_bys)),
        }
    }
}

/// 根据地理位置坐标进行分组。
#[derive(Debug, Default, Clone)]
pub struct GroupByGeoDistance {
    // 分组名称
    pub name: String,

    /// 字段名称
    pub field_name: String,

    /// 设置起始中心点坐标
    pub origin: GeoPoint,

    /// 分组的依据范围
    pub ranges: Vec<Range<f64>>,

    /// 子统计聚合Aggregation，子统计聚合会根据分组内容再进行一次统计聚合分析。
    pub sub_aggregations: Vec<Aggregation>,

    /// 子统计聚合GroupBy，子统计聚合会根据分组内容再进行一次统计聚合分析。
    pub sub_group_bys: Vec<GroupBy>,
}

impl GroupByGeoDistance {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            ..Default::default()
        }
    }

    /// 设置名称
    pub fn name(mut self, name: &str) -> Self {
        self.name = name.to_string();

        self
    }

    /// 设置字段名
    pub fn field_name(mut self, field_name: &str) -> Self {
        self.field_name = field_name.to_string();

        self
    }

    /// 设置起始中心点
    pub fn origin(mut self, origin: GeoPoint) -> Self {
        self.origin = origin;

        self
    }

    /// 增加一个分组范围
    pub fn range(mut self, range: Range<f64>) -> Self {
        self.ranges.push(range);

        self
    }

    /// 设置分组范围
    pub fn ranges(mut self, ranges: impl IntoIterator<Item = Range<f64>>) -> Self {
        self.ranges = ranges.into_iter().collect();

        self
    }

    /// 增加子聚合
    pub fn sub_aggregation(mut self, aggr: Aggregation) -> Self {
        self.sub_aggregations.push(aggr);

        self
    }

    /// 设置子聚合
    pub fn sub_aggregations(mut self, aggregations: impl IntoIterator<Item = Aggregation>) -> Self {
        self.sub_aggregations = aggregations.into_iter().collect();

        self
    }

    /// 增加子分组
    pub fn sub_group_by(mut self, sub_group_by: GroupBy) -> Self {
        self.sub_group_bys.push(sub_group_by);

        self
    }

    /// 设置子分组
    pub fn sub_group_bys(mut self, sub_group_bys: impl IntoIterator<Item = GroupBy>) -> Self {
        self.sub_group_bys = sub_group_bys.into_iter().collect();

        self
    }

    pub(crate) fn validate(&self) -> OtsResult<()> {
        if !validate_group_name(&self.name) {
            return Err(OtsError::ValidationFailed(format!("invalid group name: {}", self.name)));
        }

        if !validate_column_name(&self.field_name) {
            return Err(OtsError::ValidationFailed(format!("invalid field name: {}", self.field_name)));
        }

        if self.ranges.is_empty() {
            return Err(OtsError::ValidationFailed("ranges must not be empty".to_string()));
        }

        for g in &self.sub_group_bys {
            g.validate()?;
        }

        for a in &self.sub_aggregations {
            a.validate()?;
        }

        Ok(())
    }
}

impl From<GroupByGeoDistance> for crate::protos::search::GroupByGeoDistance {
    fn from(value: GroupByGeoDistance) -> Self {
        let GroupByGeoDistance {
            name: _,
            field_name,
            origin,
            ranges,
            sub_aggregations,
            sub_group_bys,
        } = value;

        Self {
            field_name: Some(field_name),
            origin: Some(crate::protos::search::GeoPoint::from(origin)),
            ranges: ranges.into_iter().map(|r| r.into()).collect(),
            sub_aggs: Some(crate::protos::search::Aggregations::from(sub_aggregations)),
            sub_group_bys: Some(crate::protos::search::GroupBys::from(sub_group_bys)),
        }
    }
}

/// 组合式分组
#[derive(Debug, Clone, Default)]
pub struct GroupByComposite {
    /// GroupBy 的名字，之后从 GroupBy 结果列表中根据该名字拿到 GroupBy 结果
    pub name: String,

    /// 返回的分组数量
    pub size: u32,

    /// 返回分组的数量；软限制，允许设置大于服务端最大限制值。当该值超过服务端最大值限制后被修正为最大值。
    ///
    /// - 实际值返回分组结果数量为：min(suggestedSize, 服务端分组数量限制，总分组数量)
    /// - 适用场景：重吞吐的场景，一般是对接计算系统，比如spark、presto等。
    pub suggested_size: Option<u32>,

    /// GroupByComposite 结果内会返回 `nextToken`，用于支持分组翻页
    pub next_token: Option<String>,

    /// 支持对多种类型的多列进行分组统计
    ///
    /// - [`GroupByField`]
    /// - [`GroupByHistogram`]
    /// - [`GroupByDateHistogram`]
    pub sources: Vec<GroupBy>,

    /// 子统计聚合Aggregation，子统计聚合会根据分组内容再进行一次统计聚合分析。
    pub sub_aggregations: Vec<Aggregation>,

    /// 子统计聚合GroupBy，子统计聚合会根据分组内容再进行一次统计聚合分析。
    pub sub_group_bys: Vec<GroupBy>,
}

impl GroupByComposite {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            size: 10,
            ..Default::default()
        }
    }

    /// 设置名称
    pub fn name(mut self, name: &str) -> Self {
        self.name = name.to_string();

        self
    }

    /// 设置分组数量
    pub fn size(mut self, size: u32) -> Self {
        self.size = size;

        self
    }

    /// 设置软限制分组数量
    pub fn suggested_size(mut self, suggested_size: u32) -> Self {
        self.suggested_size = Some(suggested_size);

        self
    }

    /// 添加一个分组设置
    pub fn source(mut self, source: GroupBy) -> Self {
        self.sources.push(source);

        self
    }

    /// 设置分组集合
    pub fn sources(mut self, sources: impl IntoIterator<Item = GroupBy>) -> Self {
        self.sources = sources.into_iter().collect();

        self
    }

    /// 设置分页 token
    pub fn next_token(mut self, next_token: impl Into<String>) -> Self {
        self.next_token = Some(next_token.into());

        self
    }

    /// 增加子聚合
    pub fn sub_aggregation(mut self, aggr: Aggregation) -> Self {
        self.sub_aggregations.push(aggr);

        self
    }

    /// 设置子聚合
    pub fn sub_aggregations(mut self, aggregations: impl IntoIterator<Item = Aggregation>) -> Self {
        self.sub_aggregations = aggregations.into_iter().collect();

        self
    }

    /// 增加子分组
    pub fn sub_group_by(mut self, sub_group_by: GroupBy) -> Self {
        self.sub_group_bys.push(sub_group_by);

        self
    }

    /// 设置子分组
    pub fn sub_group_bys(mut self, sub_group_bys: impl IntoIterator<Item = GroupBy>) -> Self {
        self.sub_group_bys = sub_group_bys.into_iter().collect();

        self
    }

    pub(crate) fn validate(&self) -> OtsResult<()> {
        if !validate_group_name(&self.name) {
            return Err(OtsError::ValidationFailed(format!("invalid group name: {}", self.name)));
        }

        if self.size > i32::MAX as u32 {
            return Err(OtsError::ValidationFailed("size is too large".to_string()));
        }

        if let Some(n) = self.suggested_size {
            if n > i32::MAX as u32 {
                return Err(OtsError::ValidationFailed("suggested size is too large".to_string()));
            }
        }

        if self.sources.is_empty() {
            return Err(OtsError::ValidationFailed("sources must not be empty".to_string()));
        }

        for g in &self.sources {
            g.validate()?;
        }

        for g in &self.sub_group_bys {
            g.validate()?;
        }

        for a in &self.sub_aggregations {
            a.validate()?;
        }

        Ok(())
    }
}

impl From<GroupByComposite> for crate::protos::search::GroupByComposite {
    fn from(value: GroupByComposite) -> Self {
        let GroupByComposite {
            name: _,
            size,
            suggested_size,
            next_token,
            sources,
            sub_aggregations,
            sub_group_bys,
        } = value;

        Self {
            sources: Some(crate::protos::search::GroupBys::from(sources)),
            size: Some(size as i32),
            suggested_size: suggested_size.map(|n| n as i32),
            next_token,
            sub_aggs: Some(crate::protos::search::Aggregations::from(sub_aggregations)),
            sub_group_bys: Some(crate::protos::search::GroupBys::from(sub_group_bys)),
        }
    }
}

/// 分组设置
#[derive(Debug, Clone)]
pub enum GroupBy {
    Field(GroupByField),
    Filter(GroupByFilter),
    Range(GroupByRange),
    Histogram(GroupByHistogram),
    DateHistogram(GroupByDateHistogram),
    GeoGrid(GroupByGeoGrid),
    GeoDistance(GroupByGeoDistance),
    Composite(GroupByComposite),
}

impl From<GroupBy> for crate::protos::search::GroupBy {
    fn from(value: GroupBy) -> Self {
        match value {
            GroupBy::Field(gb) => Self {
                name: Some(gb.name.clone()),
                r#type: Some(GroupByType::GroupByField as i32),
                body: Some(crate::protos::search::GroupByField::from(gb).encode_to_vec()),
            },

            GroupBy::Filter(gb) => Self {
                name: Some(gb.name.clone()),
                r#type: Some(GroupByType::GroupByFilter as i32),
                body: Some(crate::protos::search::GroupByFilter::from(gb).encode_to_vec()),
            },

            GroupBy::Histogram(gb) => Self {
                name: Some(gb.name.clone()),
                r#type: Some(GroupByType::GroupByHistogram as i32),
                body: Some(crate::protos::search::GroupByHistogram::from(gb).encode_to_vec()),
            },

            GroupBy::DateHistogram(gb) => Self {
                name: Some(gb.name.clone()),
                r#type: Some(GroupByType::GroupByDateHistogram as i32),
                body: Some(crate::protos::search::GroupByDateHistogram::from(gb).encode_to_vec()),
            },

            GroupBy::Range(gb) => Self {
                name: Some(gb.name.clone()),
                r#type: Some(GroupByType::GroupByRange as i32),
                body: Some(crate::protos::search::GroupByRange::from(gb).encode_to_vec()),
            },

            GroupBy::GeoGrid(gb) => Self {
                name: Some(gb.name.clone()),
                r#type: Some(GroupByType::GroupByGeoGrid as i32),
                body: Some(crate::protos::search::GroupByGeoGrid::from(gb).encode_to_vec()),
            },

            GroupBy::GeoDistance(gb) => Self {
                name: Some(gb.name.clone()),
                r#type: Some(GroupByType::GroupByGeoDistance as i32),
                body: Some(crate::protos::search::GroupByGeoDistance::from(gb).encode_to_vec()),
            },

            GroupBy::Composite(gb) => Self {
                name: Some(gb.name.clone()),
                r#type: Some(GroupByType::GroupByComposite as i32),
                body: Some(crate::protos::search::GroupByComposite::from(gb).encode_to_vec()),
            },
        }
    }
}

impl GroupBy {
    pub(crate) fn validate(&self) -> OtsResult<()> {
        match self {
            GroupBy::Field(gb) => gb.validate(),
            GroupBy::Filter(gb) => gb.validate(),
            GroupBy::Range(gb) => gb.validate(),
            GroupBy::Histogram(gb) => gb.validate(),
            GroupBy::DateHistogram(gb) => gb.validate(),
            GroupBy::GeoGrid(gb) => gb.validate(),
            GroupBy::GeoDistance(gb) => gb.validate(),
            GroupBy::Composite(gb) => gb.validate(),
        }
    }
}

impl<T, G> From<T> for crate::protos::search::GroupBys
where
    T: IntoIterator<Item = G>,
    G: Into<crate::protos::search::GroupBy>,
{
    fn from(value: T) -> Self {
        Self {
            group_bys: value.into_iter().map(|g| g.into()).collect(),
        }
    }
}
