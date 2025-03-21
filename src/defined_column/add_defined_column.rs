use prost::Message;
use reqwest::Method;

use crate::{
    OtsClient, OtsOp, OtsRequest, OtsResult, add_per_request_options,
    error::OtsError,
    protos::table_store::{AddDefinedColumnRequest, AddDefinedColumnResponse, DefinedColumnSchema, DefinedColumnType},
    table::rules::{validate_column_name, validate_table_name},
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

    /// 添加预定义列
    fn add_column(mut self, name: impl Into<String>, col_type: DefinedColumnType) -> Self {
        let col = DefinedColumnSchema {
            name: name.into(),
            r#type: col_type as i32,
        };

        self.columns.push(col);

        self
    }

    /// 添加一个预定义列
    pub fn column(mut self, def_col: DefinedColumnSchema) -> Self {
        self.columns.push(def_col);

        self
    }

    /// 设置预定义列
    pub fn columns(mut self, def_cols: impl IntoIterator<Item = DefinedColumnSchema>) -> Self {
        self.columns = def_cols.into_iter().collect();

        self
    }

    /// 添加整数类型预定以列
    pub fn column_integer(self, name: &str) -> Self {
        self.add_column(name, DefinedColumnType::DctInteger)
    }

    /// 添加字符串类型预定义列
    pub fn column_string(self, name: &str) -> Self {
        self.add_column(name, DefinedColumnType::DctString)
    }

    /// 添加双精度类型预定义列
    pub fn column_double(self, name: &str) -> Self {
        self.add_column(name, DefinedColumnType::DctDouble)
    }

    /// 添加布尔值类型预定义列
    pub fn column_bool(self, name: &str) -> Self {
        self.add_column(name, DefinedColumnType::DctBoolean)
    }

    /// 添加二进制类型预定义列
    pub fn column_blob(self, name: &str) -> Self {
        self.add_column(name, DefinedColumnType::DctBlob)
    }

    fn validate(&self) -> OtsResult<()> {
        if !validate_table_name(&self.table_name) {
            return Err(OtsError::ValidationFailed(format!("Invalid table name: {}", self.table_name)));
        }

        if self.columns.is_empty() {
            return Err(OtsError::ValidationFailed("Columns to add can not be empty".to_string()));
        }

        for col in &self.columns {
            if !validate_column_name(&col.name) {
                return Err(OtsError::ValidationFailed(format!("Invalid column name: {}", col.name)));
            }
        }

        Ok(())
    }

    /// 执行添加预定义列操作
    pub async fn send(self) -> OtsResult<AddDefinedColumnResponse> {
        self.validate()?;

        let Self { client, table_name, columns } = self;

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
