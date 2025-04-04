use crate::model::{Column, PrimaryKeyColumn, PrimaryKeyValue};

use super::{TimeseriesKey, parse_tags};

/// 时序表中的数据行
#[derive(Debug, Clone)]
pub struct TimeseriesRow {
    /// 时间线标识
    pub key: TimeseriesKey,

    /// 时间戳（微秒）
    pub timestamp_us: i64,

    /// 列数据
    pub fields: Vec<Column>,
}

// impl TryFrom<crate::protos::timeseries::TimeseriesRows> for Vec<TimeseriesRow> {
//     type Error = OtsError;

//     fn try_from(value: crate::protos::timeseries::TimeseriesRows) -> Result<Self, Self::Error> {
//         let crate::protos::timeseries::TimeseriesRows {
//             r#type,
//             rows_data,
//             flatbuffer_crc32c,
//         } = value;

//         if rows_data.is_empty() {
//             return Ok(vec![])
//         }

//         let ser_type = match RowsSerializeType::try_from(r#type) {
//             Ok(t) => t,
//             Err(err) => return Err(OtsError::ValidationFailed(format!("invalid timeseries serialize type: {}", r#type))),
//         };

//         log::debug!("timeseries serialize type: {}", r#type);

//         let mut rows = vec![];

//         Ok(rows)
//     }
// }

/// 从宽表行转换出来时序行
impl From<crate::model::Row> for TimeseriesRow {
    fn from(value: crate::model::Row) -> Self {
        let crate::model::Row {
            primary_key,
            columns,
            deleted: _,
        } = value;

        let mut key = TimeseriesKey::default();
        let mut timestamp_us = 0;

        for column in primary_key.columns {
            let PrimaryKeyColumn { name, value } = column;

            match name.as_str() {
                "_m_name" => {
                    if let PrimaryKeyValue::String(s) = value {
                        key.measurement_name = Some(s);
                    }
                }

                "_data_source" => {
                    if let PrimaryKeyValue::String(s) = value {
                        key.datasource = Some(s);
                    }
                }

                "_tags" => {
                    if let PrimaryKeyValue::String(s) = value {
                        key.tags = parse_tags(&s);
                    }
                }

                "_time" => {
                    if let PrimaryKeyValue::Integer(ts) = value {
                        timestamp_us = ts;
                    }
                }

                _ => {}
            }
        }

        Self {
            key,
            timestamp_us,
            fields: columns,
        }
    }
}
