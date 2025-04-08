use flatbuffers::FlatBufferBuilder;

use crate::{error::OtsError, model::{Column, ColumnValue, PrimaryKey, PrimaryKeyColumn, PrimaryKeyValue}, protos::fbs::timeseries::{DataType, FieldValuesBuilder}, OtsResult};

use super::{parse_tags, rules::validate_timeseries_field_name, TimeseriesKey, TimeseriesVersion};

/// 时序表中的数据行
#[derive(Debug, Default, Clone)]
pub struct TimeseriesRow {
    /// 时间线标识
    pub key: TimeseriesKey,

    /// 时间戳（微秒）
    pub timestamp_us: u64,

    /// 列数据
    pub fields: Vec<Column>,
}

impl TimeseriesRow {
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置度量名称
    pub fn measurement_name(mut self, measurement: impl Into<String>) -> Self {
        self.key.measurement_name = Some(measurement.into());
        self
    }

    /// 设置源
    pub fn datasource(mut self, source: impl Into<String>) -> Self {
        self.key.datasource = Some(source.into());
        self
    }

    /// 增加一个 `supported_table_version` 为 `1` 的实例的标签
    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.key.tags.insert(key.into(), value.into());
        self
    }

    /// 设置一个 `supported_table_version` 为 `1` 的实例的所有标签
    pub fn tags(mut self, tags: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>) -> Self {
        self.key.tags = tags.into_iter().map(|(k, v)| (k.into(), v.into())).collect();
        self
    }

    /// 设置时间戳，微秒为单位
    pub fn timestamp_us(mut self, ts_us: u64) -> Self {
        self.timestamp_us = ts_us;

        self
    }

    pub fn field(mut self, col: Column) -> Self {
        self.fields.push(col);

        self
    }

    pub fn fields(mut self, cols: impl IntoIterator<Item = Column>) -> Self {
        self.fields = cols.into_iter().collect();

        self
    }

    /// 添加/更新字符串类型的列
    pub fn field_string(mut self, name: &str, value: impl Into<String>) -> Self {
        self.fields.push(Column::from_string(name, value));

        self
    }

    /// 添加/更新整数列
    pub fn field_integer(mut self, name: &str, value: i64) -> Self {
        self.fields.push(Column::from_integer(name, value));

        self
    }

    /// 添加/更新双精度列
    pub fn field_double(mut self, name: &str, value: f64) -> Self {
        self.fields.push(Column::from_double(name, value));

        self
    }

    /// 添加/更新布尔值列
    pub fn field_bool(mut self, name: &str, value: bool) -> Self {
        self.fields.push(Column::from_bool(name, value));

        self
    }

    /// 添加/更新二进制列
    pub fn field_blob(mut self, name: &str, value: impl Into<Vec<u8>>) -> Self {
        self.fields.push(Column::from_blob(name, value));

        self
    }

    pub(crate) fn validate(&self) -> OtsResult<()> {
        for f in &self.fields {
            if !validate_timeseries_field_name(&f.name) {
                return Err(OtsError::ValidationFailed(format!("invalid field name: {}", f.name)));
            }
        }

        if self.fields.len() > super::rules::MAX_FIELD_COUNT {
            return Err(OtsError::ValidationFailed(format!("invalid field. field count exceeds max field count: {}", super::rules::MAX_FIELD_COUNT)));
        }

        Ok(())
    }

    /// Write a row to flat buffer
    pub(crate) fn write_flat_buffer(&self, fbb: &mut FlatBufferBuilder, ver: TimeseriesVersion) -> OtsResult<()> {
        let mut field_types = vec![DataType::NONE; self.fields.len()];
        let mut field_names = vec![""; self.fields.len()];

        let measure_name = if let Some(s) = &self.key.measurement_name {
            fbb.create_string(s.as_str())
        } else {
            fbb.create_string("")
        };



        for col in &self.fields {
            match &col.value {
                ColumnValue::Integer(n) => {
                    let v = fbb.create_vector(&[*n]);
                    // fv_builder.add_long_values(v);
                },
                ColumnValue::Double(d) => {
                    let v = fbb.create_vector(&[*d]);
                    // fv_builder.add_double_values(v);
                },
                ColumnValue::Boolean(b) => {
                    let v = fbb.create_vector(&[*b]);
                    // fv_builder.add_bool_values(v);
                },
                ColumnValue::String(s) => {
                    let fbs = fbb.create_string(s.as_str());
                    let ss = fbb.create_vector(&[fbs]);
                    // fv_builder.add_string_values(ss);
                },
                ColumnValue::Blob(items) => todo!(),
                other => {
                    return Err(OtsError::ValidationFailed(format!("invalid column data type: {:?}", other)));
                }
            }
        }

        let mut fv_builder = FieldValuesBuilder::new(fbb);

        Ok(())
    }
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
            timestamp_us: timestamp_us as u64,
            fields: columns,
        }
    }
}


impl From<TimeseriesRow> for crate::model::Row {
    fn from(value: TimeseriesRow) -> Self {
        let TimeseriesRow {
            key,
            timestamp_us,
            fields,
        } = value;

        let TimeseriesKey { measurement_name, datasource, tags } = key;

        let mut primary_key = PrimaryKey::new();

        if let Some(s) = measurement_name {
            primary_key = primary_key.column_string("_m_name", s);
        }

        if let Some(s) = datasource {
            primary_key = primary_key.column_string("_data_source", s);
        }

        if !tags.is_empty() {
            let mut items = tags.into_iter().collect::<Vec<_>>();
            items.sort_by(|a, b| a.0.cmp(&b.0));

            primary_key = primary_key.column_string(
                "_tags",
                format!(
                    "[{}]",
                    items.into_iter().map(|(k, v)| format!("\"{}={}\"", k, v)).collect::<Vec<_>>().join(",")
                )
            );
        }

        primary_key = primary_key.column_integer("_time", timestamp_us as i64);

        Self {
            primary_key,
            columns: fields,
            ..Default::default()
        }

    }
}

/// 这里没有处理 MIN 和 MAX 的数据...
impl From<&ColumnValue> for DataType {
    fn from(value: &ColumnValue) -> Self {
        match value {
            ColumnValue::Null => DataType::NONE,
            ColumnValue::Integer(_) => DataType::LONG,
            ColumnValue::Double(_) => DataType::DOUBLE,
            ColumnValue::Boolean(_) => DataType::BOOLEAN,
            ColumnValue::String(_) => DataType::STRING,
            ColumnValue::Blob(_) => DataType::BINARY,
            _ => DataType::NONE,
        }
    }
}

/// 将时序表的行集合以 flat buffer 的格式编码
pub(crate) fn encode_rows_to_flat_buffer(rows: &[TimeseriesRow], supported_table_version: TimeseriesVersion) -> Vec<u8> {
    if rows.is_empty() {
        return vec![];
    }

    // flat buffer 编码的 `FlatBufferRowGroup` 是以度量名称分组的，并且，字段的值和类型是两个集合保存的，
    // 为了避免多个相同度量名称的行具有不同的列，所以还是一行对一行的这么转换
    let mut fbb = FlatBufferBuilder::new();

    for row in rows {
        write_row_to_flat_buffer(&mut fbb, row);
    }

    vec![]
}

pub(crate) fn write_row_to_flat_buffer(fbb: &mut FlatBufferBuilder, row: &TimeseriesRow) {

}
