use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt};
use prost::Message;

use crate::{
    OtsClient, OtsOp, OtsRequest, OtsResult, add_per_request_options,
    error::OtsError,
    model::{PrimaryKey, PrimaryKeyColumn, PrimaryKeyValue, Row},
    protos::{
        plain_buffer::{HEADER, MASK_HEADER, MASK_ROW_CHECKSUM},
        table_store::{ConsumedCapacity, GetRowRequest, TimeRange},
    },
};

/// 根据指定的主键读取单行数据。
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/getrow>
#[derive(Default, Debug, Clone)]
pub struct GetRowOperation {
    client: OtsClient,
    pub table_name: String,
    pub pk_values: Vec<PrimaryKeyColumn>,
    pub columns: Vec<String>,

    // Time range fields
    pub time_range_start_ms: Option<i64>,
    pub time_range_end_ms: Option<i64>,
    pub time_range_specific_ms: Option<i64>,

    pub max_versions: Option<i32>,
    pub start_column: Option<String>,
    pub end_column: Option<String>,
    pub transaction_id: Option<String>,
}

add_per_request_options!(GetRowOperation);

impl GetRowOperation {
    pub(crate) fn new(client: OtsClient, table_name: &str) -> Self {
        Self {
            client,
            table_name: table_name.to_string(),
            ..Default::default()
        }
    }

    /// 添加字符串类型的主键查询值
    pub fn add_string_pk_value(mut self, pk_name: &str, pk_value: impl Into<String>) -> Self {
        self.pk_values.push(PrimaryKeyColumn {
            name: pk_name.to_string(),
            value: PrimaryKeyValue::String(pk_value.into()),
        });
        self
    }

    /// 添加整数类型的主键查询值
    pub fn add_integer_pk_value(mut self, pk_name: &str, pk_value: i64) -> Self {
        self.pk_values.push(PrimaryKeyColumn {
            name: pk_name.to_string(),
            value: PrimaryKeyValue::Integer(pk_value),
        });

        self
    }

    /// 添加二进制类型的主键查询值
    pub fn add_binary_pk_value(mut self, pk_name: &str, pk_value: impl Into<Vec<u8>>) -> Self {
        self.pk_values.push(PrimaryKeyColumn {
            name: pk_name.to_string(),
            value: PrimaryKeyValue::Binary(pk_value.into()),
        });

        self
    }

    /// 需要返回的全部列的列名。如果为空，则返回指定行的所有列。`columns_to_get` 个数不应超过128个。
    /// 如果指定的列不存在，则不会返回指定列的数据；如果给出了重复的列名，返回结果只会包含一次指定列。
    pub fn add_column_to_get(mut self, col_name: &str) -> Self {
        self.columns.push(col_name.to_string());

        self
    }

    /// 读取数据时，返回的最多版本个数。和 `time_range` 至少存在一个
    /// 如果指定 `max_versions` 为 `2` ，则每一列最多返回 `2` 个版本的数据。
    pub fn max_versions(mut self, max_versions: i32) -> Self {
        self.max_versions = Some(max_versions);

        self
    }

    /// 查询数据时指定的时间戳范围 `[start_time, end_time)` 或特定时间戳值 `time_specific`。
    /// 时间范围和特定时间戳值二者指定其一即可。
    ///
    /// - `start_ms`: 起始时间戳。单位是毫秒。时间戳的取值最小值为 `0`，最大值为 [`i64::MAX`](`std::i64::MAX`)。
    /// - `end_ms`: 结束时间戳。单位是毫秒。时间戳的取值最小值为 `0`，最大值为 [`i64::MAX`](`std::i64::MAX`)。
    pub fn time_range(mut self, start_ms: i64, end_ms: i64) -> Self {
        self.time_range_start_ms = Some(start_ms);
        self.time_range_end_ms = Some(end_ms);

        self
    }

    /// 指定精确的时间戳
    pub fn specific_time_ms(mut self, time_ms: i64) -> Self {
        self.time_range_specific_ms = Some(time_ms);

        self
    }

    /// 指定读取时的起始列，主要用于宽行读。列的顺序按照列名的字典序排序。返回的结果中**包含**当前起始列。
    /// 如果一张表有 `a` 、 `b` 、 `c` 三列，读取时指定 `start_column` 为 `b` ，则会从 `b` 列开始读，返回 `b`、`c` 两列。
    pub fn start_column(mut self, col_name: impl Into<String>) -> Self {
        self.start_column = Some(col_name.into());

        self
    }

    /// 返回的结果中**不包含**当前结束列。列的顺序按照列名的字典序排序。
    /// 如果一张表有 `a` 、 `b` 、 `c` 三列，读取时指定 `end_column` 为 `b`，则读到 `b` 列时会结束，返回 `a` 列。
    pub fn end_column(mut self, col_name: impl Into<String>) -> Self {
        self.end_column = Some(col_name.into());

        self
    }

    /// 局部事务ID。当使用局部事务功能读取数据时必须设置此参数。
    pub fn transaction_id(mut self, tx_id: impl Into<String>) -> Self {
        self.transaction_id = Some(tx_id.into());

        self
    }

    pub async fn send(self) -> OtsResult<GetRowResponse> {
        let Self {
            client,
            table_name,
            pk_values,
            columns,
            time_range_start_ms,
            time_range_end_ms,
            time_range_specific_ms,
            max_versions,
            start_column,
            end_column,
            transaction_id,
        } = self;

        let pk = PrimaryKey { keys: pk_values };

        let pk_bytes = pk.encode_plain_buffer(MASK_HEADER | MASK_ROW_CHECKSUM);

        let msg = GetRowRequest {
            table_name,
            primary_key: pk_bytes,
            columns_to_get: columns,
            time_range: if time_range_start_ms.is_some() || time_range_end_ms.is_some() || time_range_specific_ms.is_some() {
                Some(TimeRange {
                    start_time: time_range_start_ms,
                    end_time: time_range_end_ms,
                    specific_time: time_range_specific_ms,
                })
            } else {
                None
            },
            max_versions,
            filter: None,
            start_column,
            end_column,
            token: None,
            transaction_id,
        };

        if msg.max_versions.is_none() && msg.time_range.is_none() {
            return Err(OtsError::ValidationFailed("time_range 和 max_versions 至少指定一个".to_string()));
        }

        let req = OtsRequest {
            operation: OtsOp::GetRow,
            body: msg.encode_to_vec(),
            ..Default::default()
        };

        let response = client.send(req).await?;
        GetRowResponse::decode(response.bytes().await?.to_vec())
    }
}

#[derive(Clone, Default, Debug)]
pub struct GetRowResponse {
    pub consumed: ConsumedCapacity,
    pub row: Option<Row>,
    pub next_token: Option<Vec<u8>>,
}

impl GetRowResponse {
    pub fn decode(bytes: Vec<u8>) -> OtsResult<Self> {
        let msg = crate::protos::table_store::GetRowResponse::decode(bytes.as_slice())?;
        let crate::protos::table_store::GetRowResponse {
            consumed,
            row: row_bytes,
            next_token,
        } = msg;

        let row = if !row_bytes.is_empty() {
            let mut cursor = Cursor::new(row_bytes);
            let header = cursor.read_u32::<LittleEndian>()?;

            if header != HEADER {
                return Err(OtsError::PlainBufferError(format!("invalid message header: {}", header)));
            }

            Some(Row::read_plain_buffer(&mut cursor)?)
        } else {
            None
        };

        Ok(Self { consumed, row, next_token })
    }
}
