use prost::Message;
use reqwest::Method;

use crate::{
    OtsClient, OtsOp, OtsRequest, OtsResult, add_per_request_options,
    error::OtsError,
    protos::table_store::{AddDefinedColumnRequest, AddDefinedColumnResponse, DefinedColumnSchema, DefinedColumnType},
};

/// 添加预定义列
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/adddefinedcolumn>
#[derive(Default, Debug, Clone)]
pub struct AddDefinedColumnOperation {
    client: OtsClient,
    pub table_name: String,
    pub columns: Vec<DefinedColumnSchema>,
}

add_per_request_options!(AddDefinedColumnOperation);

impl AddDefinedColumnOperation {
    pub(crate) fn new(client: OtsClient, table_name: &str) -> Self {
        Self {
            client,
            table_name: table_name.to_string(),
            columns: vec![],
        }
    }

    /// Add a column to the table.
    pub fn add_column(mut self, col_name: impl Into<String>, col_type: DefinedColumnType) -> Self {
        self.columns.push(DefinedColumnSchema {
            name: col_name.into(),
            r#type: col_type as i32,
        });
        self
    }

    /// 添加整数类型预定义列
    pub fn add_integer_column(self, name: impl Into<String>) -> Self {
        self.add_column(name, DefinedColumnType::DctInteger)
    }

    /// 添加字符串类型预定义列
    pub fn add_string_column(self, name: impl Into<String>) -> Self {
        self.add_column(name, DefinedColumnType::DctString)
    }

    /// 添加双精度类型预定义列
    pub fn add_double_column(self, name: impl Into<String>) -> Self {
        self.add_column(name, DefinedColumnType::DctDouble)
    }

    /// 添加布尔值类型预定义列
    pub fn add_boolean_column(self, name: impl Into<String>) -> Self {
        self.add_column(name, DefinedColumnType::DctBoolean)
    }

    /// 添加二进制类型预定义列
    pub fn add_blob_column(self, name: impl Into<String>) -> Self {
        self.add_column(name, DefinedColumnType::DctBlob)
    }

    /// 执行添加预定义列操作
    pub async fn send(self) -> OtsResult<AddDefinedColumnResponse> {
        let Self { client, table_name, columns } = self;

        if columns.is_empty() {
            return Err(OtsError::ValidationFailed("No columns to add".to_string()));
        }

        let msg = AddDefinedColumnRequest { table_name, columns };

        let req = OtsRequest {
            method: Method::POST,
            operation: OtsOp::AddDefinedColumn,
            body: msg.encode_to_vec(),
            ..Default::default()
        };

        let response = client.send(req).await?;
        Ok(AddDefinedColumnResponse::decode(response.bytes().await?)?)
    }
}
