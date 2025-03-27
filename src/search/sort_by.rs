use std::fmt::Display;

use crate::{model::ColumnValue, protos::search::{DocSort, GeoDistanceType, PrimaryKeySort, ScoreSort, SortMode, SortOrder}};

use super::NestedFilter;



/// 坐标点，是一个经纬度值。
#[derive(Debug, Default, Clone, Copy)]
pub struct GeoPoint {
    /// 纬度
    pub latitude: i64,

    /// 经度
    pub longitude: i64,
}

impl GeoPoint {
    pub fn new(lat: i64, lng: i64) -> Self {
        Self {
            latitude: lat,
            longitude: lng
        }
    }
}

impl Display for GeoPoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{},{}", self.latitude, self.longitude)
    }
}

/// 多元索引中字段排序方式的配置。
#[derive(Debug, Default, Clone)]
pub struct FieldSort {
    /// 排序字段名称。
    pub field_name: String,

    /// 排序的顺序，支持升序排列和降序排列，默认为升序排列。
    pub order: Option<SortOrder>,

    /// 当字段存在多个值时的排序方式。只有当字段类型为数组类型时，才需要设置此参数。
    pub mode: Option<SortMode>,

    /// 嵌套类型的过滤条件。只有当字段类型为嵌套类型时，才需要设置此参数。
    pub nested_filter: Option<NestedFilter>,

    /// 当字段不存在时使用的字段默认值。
    pub missing_value: Option<ColumnValue>,

    /// 当指定字段不存在时使用的排序字段。
    pub missing_field: Option<String>,
}

impl FieldSort {
    pub fn new(field_name: &str) -> Self {
        Self {
            field_name: field_name.to_string(),
            ..Default::default()
        }
    }

    /// 设置字段名称
    pub fn field_name(mut self, field_name: &str) -> Self {
        self.field_name = field_name.to_string();

        self
    }

    /// 设置排序顺序
    pub fn order(mut self, order: SortOrder) -> Self {
        self.order = Some(order);

        self
    }

    /// 设置多值字段的排序方式
    pub fn mode(mut self, mode: SortMode) -> Self {
        self.mode = Some(mode);

        self
    }

    /// 设置嵌套类型的过滤条件
    pub fn nested_filter(mut self, filter: NestedFilter) -> Self {
        self.nested_filter = Some(filter);

        self
    }

    /// 设置排序字段不存在时的默认值
    pub fn missing_value(mut self, value: ColumnValue) -> Self {
        self.missing_value = Some(value);

        self
    }

    /// 设置排序字段不存在时的备选字段
    pub fn missing_field(mut self, missing_field: impl Into<String>) -> Self {
        self.missing_field = Some(missing_field.into());

        self
    }
}

impl From<FieldSort> for crate::protos::search::FieldSort {
    fn from(value: FieldSort) -> Self {
        let FieldSort {
            field_name,
            order,
            mode,
            nested_filter,
            missing_value,
            missing_field,
        } = value;

        crate::protos::search::FieldSort {
            field_name: Some(field_name),
            order: order.map(|o| o as i32),
            mode: mode.map(|m| m as i32),
            nested_filter: nested_filter.map(|nf| nf.into()),
            missing_value: missing_value.map(|v| v.encode_plain_buffer()),
            missing_field,
        }
    }
}

/// 地理位置排序方式
#[derive(Debug, Default, Clone)]
pub struct GeoDistanceSort {
    /// 排序字段名称。
    pub field_name: String,

    /// 中心坐标点，是一个经纬度值。
    pub points: Vec<GeoPoint>,

    /// 排序的顺序，支持升序排列和降序排列，默认为升序排列。
    pub order: Option<SortOrder>,

    /// 当字段存在多个值时的排序方式。只有当字段类型为数组类型时，才需要设置此参数。
    pub mode: Option<SortMode>,

    /// 距离计算方式。
    pub distance_type: Option<GeoDistanceType>,

    /// 嵌套类型的过滤条件。只有当字段类型为嵌套类型时，才需要设置此参数。
    pub nested_filter: Option<NestedFilter>,
}

impl GeoDistanceSort {
    /// 构造实例之后，还需要设置中心点
    pub fn new(field_name: &str) -> Self {
        Self {
            field_name: field_name.to_string(),
            ..Default::default()
        }
    }

    /// 设置字段名称
    pub fn field_name(mut self, field_name: &str) -> Self {
        self.field_name = field_name.to_string();

        self
    }

    /// 添加 1 个中心点
    pub fn point(mut self, point: GeoPoint) -> Self {
        self.points.push(point);

        self
    }

    /// 设置多个中心点
    pub fn points(mut self, points: impl IntoIterator<Item = GeoPoint>) -> Self {
        self.points = points.into_iter().collect();

        self
    }

    /// 设置排序顺序
    pub fn order(mut self, order: SortOrder) -> Self {
        self.order = Some(order);

        self
    }

    /// 设置多值字段的排序方式
    pub fn mode(mut self, mode: SortMode) -> Self {
        self.mode = Some(mode);

        self
    }

    /// 设置嵌套类型的过滤条件
    pub fn nested_filter(mut self, filter: NestedFilter) -> Self {
        self.nested_filter = Some(filter);

        self
    }

    /// 设置距离计算方式
    pub fn distance_type(mut self, distanct_type: GeoDistanceType) -> Self {
        self.distance_type = Some(distanct_type);

        self
    }
}

impl From<GeoDistanceSort> for crate::protos::search::GeoDistanceSort {
    fn from(value: GeoDistanceSort) -> Self {
        let GeoDistanceSort {
            field_name,
            points,
            order,
            mode,
            distance_type,
            nested_filter,
        } = value;

        crate::protos::search::GeoDistanceSort {
            field_name: Some(field_name),
            points: points.into_iter().map(|p| format!("{}", p)).collect(),
            order: order.map(|o| o as i32),
            mode: mode.map(|m| m as i32),
            distance_type: distance_type.map(|d| d as i32),
            nested_filter: nested_filter.map(|f| f.into()),
        }
    }
}

/// 索引的排序方式
#[derive(Debug, Clone)]
pub enum Sorter {
    /// 主键排序方式。
    PrimaryKey(SortOrder),

    /// 分数排序方式。
    Score(SortOrder),

    /// 按照数据行在多元索引中的存储顺序排序
    DocSort(SortOrder),

    /// 字段值排序方式。
    Field(FieldSort),

    /// 地理位置排序方式。
    GeoDistance(GeoDistanceSort),
}

impl From<Sorter> for crate::protos::search::Sorter {
    fn from(value: Sorter) -> Self {
        let mut ret = crate::protos::search::Sorter::default();

        match value {
            Sorter::PrimaryKey(sort_order) => {
                ret.pk_sort = Some(PrimaryKeySort {
                    order: Some(sort_order as i32),
                });
            },
            Sorter::Score(sort_order) => {
                ret.score_sort = Some(ScoreSort {
                    order: Some(sort_order as i32),
                });
            },
            Sorter::DocSort(sort_order) => {
                ret.doc_sort = Some(DocSort {
                    order: Some(sort_order as i32),
                });
            },
            Sorter::Field(field_sort) => {
                ret.field_sort = Some(crate::protos::search::FieldSort::from(field_sort));
            },
            Sorter::GeoDistance(geo_distance_sort) => {
                ret.geo_distance_sort = Some(crate::protos::search::GeoDistanceSort::from(geo_distance_sort));
            },
        }

        ret
    }
}

impl<T, S> From<T> for crate::protos::search::Sort
where
    T: IntoIterator<Item = S>,
    S: Into<crate::protos::search::Sorter>
{
    fn from(value: T) -> Self {
        crate::protos::search::Sort {
            sorter: value.into_iter().map(|i| i.into()).collect(),
            disable_default_pk_sorter: Some(false),
        }
    }
}
