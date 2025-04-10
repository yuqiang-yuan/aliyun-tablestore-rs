use prost::Message;

use crate::protos::timeseries::{MetaQueryConditionType, MetaQuerySingleOperator};

/// 度量名称查询。
/// 变体中的 `String` 是指查询值
#[derive(Debug, Clone)]
pub enum MeasurementMetaQuery {
    Equal(String),
    GreaterThan(String),
    GreaterEqual(String),
    LessThan(String),
    LessEqual(String),
    Prefix(String)
}

impl From<MeasurementMetaQuery> for crate::protos::timeseries::MetaQueryMeasurementCondition {
    fn from(value: MeasurementMetaQuery) -> Self {
        match value {
            MeasurementMetaQuery::Equal(value) => Self {
                op: MetaQuerySingleOperator::OpEqual as i32,
                value,
            },

            MeasurementMetaQuery::GreaterThan(value) => Self {
                op: MetaQuerySingleOperator::OpGreaterThan as i32,
                value,
            },

            MeasurementMetaQuery::GreaterEqual(value) => Self {
                op: MetaQuerySingleOperator::OpGreaterEqual as i32,
                value,
            },

            MeasurementMetaQuery::LessThan(value) => Self {
                op: MetaQuerySingleOperator::OpLessThan as i32,
                value,
            },

            MeasurementMetaQuery::LessEqual(value) => Self {
                op: MetaQuerySingleOperator::OpLessEqual as i32,
                value,
            },

            MeasurementMetaQuery::Prefix(value) => Self {
                op: MetaQuerySingleOperator::OpPrefix as i32,
                value,
            },
        }
    }
}

/// 数据源查询。
/// 变体中的 `String` 是指查询值
#[derive(Debug, Clone)]
pub enum DatasourceMetaQuery {
    Equal(String),
    GreaterThan(String),
    GreaterEqual(String),
    LessThan(String),
    LessEqual(String),
    Prefix(String)
}

/// 标签查询。
/// 变体中的 `.0` 是查询的标签名，`.1` 是查询的标签值
#[derive(Debug, Clone)]
pub enum TagMetaQuery {
    Equal(String, String),
    GreaterThan(String, String),
    GreaterEqual(String, String),
    LessThan(String, String),
    LessEqual(String, String),
    Prefix(String, String)
}

/// 元数据查询
#[derive(Debug, Clone)]
pub enum MetaQuery {
    Measurement(MeasurementMetaQuery),
}

impl From<MetaQuery> for crate::protos::timeseries::MetaQueryCondition {
    fn from(value: MetaQuery) -> Self {
        match value {
            MetaQuery::Measurement(q) => Self {
                r#type: MetaQueryConditionType::MeasurementCondition as i32,
                proto_data: crate::protos::timeseries::MetaQueryMeasurementCondition::from(q).encode_to_vec(),
            },
        }
    }
}
