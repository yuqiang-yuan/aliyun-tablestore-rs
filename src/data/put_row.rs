use prost::Message;

use crate::{
    OtsClient, OtsOp, OtsRequest, OtsResult, add_per_request_options,
    error::OtsError,
    model::{Filter, Row},
    protos::{
        plain_buffer::{MASK_HEADER, MASK_ROW_CHECKSUM},
        table_store::{Condition, ConsumedCapacity, ReturnContent, ReturnType, RowExistenceExpectation},
    },
    table::rules::{validate_column_name, validate_table_name},
};

/// 写入行数据的请求
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/putrow>
#[derive(Debug, Default, Clone)]
pub struct PutRowRequest {
    pub table_name: String,
    pub row: Row,

    /// 在数据写入前是否进行存在性检查。取值范围如下：
    ///
    /// - `Ignore`（默认）：不做行存在性检查。
    /// - `ExpectExist` ：期望行存在。
    /// - `ExpectNotExist` ：期望行不存在。
    pub row_condition: RowExistenceExpectation,

    /// 进行行存在性检查的时候，可以附加列过滤器
    pub column_condition: Option<Filter>,

    /// 返回数据设置。目前仅支持返回主键，主要用于主键列自增功能。
    ///
    /// 见 [`ReturnType`](`crate::protos::table_store::ReturnType`)
    pub return_type: Option<ReturnType>,

    /// 如果需要返回数据，可以指定要返回的列
    pub return_columns: Vec<String>,

    /// 局部事务ID。当使用局部事务功能写入数据时必须设置此参数。
    pub transaction_id: Option<String>,
}

impl PutRowRequest {
    pub fn new(table_name: &str) -> Self {
        Self {
            table_name: table_name.to_string(),
            ..Default::default()
        }
    }

    /// 设置表名
    pub fn table_name(mut self, table_name: &str) -> Self {
        self.table_name = table_name.to_string();

        self
    }

    /// 设置要写入的行数据
    pub fn row(mut self, row: Row) -> Self {
        self.row = row;

        self
    }

    /// 设置行存在性检查
    pub fn row_condition(mut self, row_condition: RowExistenceExpectation) -> Self {
        self.row_condition = row_condition;

        self
    }

    /// 设置行存在性检查中的过滤器
    pub fn column_condition(mut self, col_condition: Filter) -> Self {
        self.column_condition = Some(col_condition);

        self
    }

    /// 设置返回值类型
    pub fn return_type(mut self, return_type: ReturnType) -> Self {
        self.return_type = Some(return_type);

        self
    }

    /// 添加一个要返回的列
    pub fn return_column(mut self, col_name: &str) -> Self {
        self.return_columns.push(col_name.into());

        self
    }

    /// 设置要返回的列
    pub fn return_columns(mut self, col_names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.return_columns = col_names.into_iter().map(|s| s.into()).collect();

        self
    }

    /// 设置事务 ID
    pub fn transaction_id(mut self, tx_id: impl Into<String>) -> Self {
        self.transaction_id = Some(tx_id.into());

        self
    }

    fn validate(&self) -> OtsResult<()> {
        if !validate_table_name(&self.table_name) {
            return Err(OtsError::ValidationFailed(format!("invalid table name: {}", self.table_name)));
        }

        if self.row.primary_key.columns.is_empty() {
            return Err(OtsError::ValidationFailed("invalid primary keys: empty".to_string()));
        }

        for key_col in &self.row.primary_key.columns {
            if !validate_column_name(&key_col.name) {
                return Err(OtsError::ValidationFailed(format!("invalid primary key name: {}", key_col.name)));
            }
        }

        for col in &self.row.columns {
            if !validate_column_name(&col.name) {
                return Err(OtsError::ValidationFailed(format!("invalid column name: {}", col.name)));
            }
        }

        Ok(())
    }
}

impl From<PutRowRequest> for crate::protos::table_store::PutRowRequest {
    fn from(value: PutRowRequest) -> crate::protos::table_store::PutRowRequest {
        let PutRowRequest {
            table_name,
            row,
            row_condition,
            column_condition,
            return_type,
            return_columns,
            transaction_id,
        } = value;

        let row_bytes = row.encode_plain_buffer(MASK_HEADER | MASK_ROW_CHECKSUM);

        crate::protos::table_store::PutRowRequest {
            table_name,
            row: row_bytes,
            condition: Condition {
                row_existence: row_condition as i32,
                column_condition: if let Some(f) = column_condition {
                    let filter_msg: crate::protos::table_store_filter::Filter = f.into();
                    Some(filter_msg.encode_to_vec())
                } else {
                    None
                },
            },
            return_content: if return_type.is_some() || !return_columns.is_empty() {
                Some(ReturnContent {
                    return_type: return_type.map(|t| t as i32),
                    return_column_names: return_columns,
                })
            } else {
                None
            },
            transaction_id,
        }
    }
}

/// 写入行数据的响应
#[derive(Debug, Clone, Default)]
pub struct PutRowResponse {
    pub consumed: ConsumedCapacity,
    pub row: Option<Row>,
}

impl TryFrom<crate::protos::table_store::PutRowResponse> for PutRowResponse {
    type Error = OtsError;

    fn try_from(value: crate::protos::table_store::PutRowResponse) -> Result<Self, Self::Error> {
        let crate::protos::table_store::PutRowResponse { consumed, row } = value;

        let row = if let Some(row_bytes) = row {
            Some(Row::decode_plain_buffer(row_bytes, MASK_HEADER)?)
        } else {
            None
        };

        Ok(Self { consumed, row })
    }
}

/// 插入数据到指定的行
#[derive(Debug, Clone, Default)]
pub struct PutRowOperation {
    client: OtsClient,
    request: PutRowRequest,
}

add_per_request_options!(PutRowOperation);

impl PutRowOperation {
    pub(crate) fn new(client: OtsClient, request: PutRowRequest) -> Self {
        Self { client, request }
    }

    /// 执行写入数据操作
    pub async fn send(self) -> OtsResult<PutRowResponse> {
        self.request.validate()?;

        let Self { client, request } = self;

        let msg: crate::protos::table_store::PutRowRequest = request.into();

        let req = OtsRequest {
            operation: OtsOp::PutRow,
            body: msg.encode_to_vec(),
            ..Default::default()
        };

        let response = client.send(req).await?;

        let response_msg = crate::protos::table_store::PutRowResponse::decode(response.bytes().await?)?;

        response_msg.try_into()
    }
}
