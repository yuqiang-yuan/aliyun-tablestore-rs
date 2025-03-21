use prost::Message;

use crate::{
    OtsClient, OtsOp, OtsRequest, OtsResult, add_per_request_options,
    error::OtsError,
    protos::table_store::{DeleteDefinedColumnRequest, DeleteDefinedColumnResponse},
    table::rules::validate_table_name,
};

/// 删除预定义列
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/deletedefinedcolumn>
#[derive(Default, Debug, Clone)]
pub struct DeleteDefinedColumnOperation {
    client: OtsClient,
    pub table_name: String,
    pub columns: Vec<String>,
}

add_per_request_options!(DeleteDefinedColumnOperation);

impl DeleteDefinedColumnOperation {
    pub(crate) fn new(client: OtsClient, table_name: &str) -> Self {
        Self {
            client,
            table_name: table_name.to_string(),
            columns: Vec::new(),
        }
    }

    /// 添加一个要删除的列的名字
    pub fn column(mut self, col_name: &str) -> Self {
        self.columns.push(col_name.into());

        self
    }

    /// 设置要删除的列的名字
    pub fn columns(mut self, col_names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = col_names.into_iter().map(|s| s.into()).collect();

        self
    }

    fn validate(&self) -> OtsResult<()> {
        if !validate_table_name(&self.table_name) {
            return Err(OtsError::ValidationFailed(format!("Invalid table name: {}", self.table_name)));
        }

        if self.columns.is_empty() {
            return Err(OtsError::ValidationFailed("Columns to delete can not be empty".to_string()));
        }

        Ok(())
    }

    pub async fn send(self) -> OtsResult<DeleteDefinedColumnResponse> {
        self.validate()?;

        let Self { client, table_name, columns } = self;

        let msg = DeleteDefinedColumnRequest { table_name, columns };

        let req = OtsRequest {
            operation: OtsOp::DeleteDefinedColumn,
            body: msg.encode_to_vec(),
            ..Default::default()
        };

        let response = client.send(req).await?;
        Ok(DeleteDefinedColumnResponse::decode(response.bytes().await?)?)
    }
}
