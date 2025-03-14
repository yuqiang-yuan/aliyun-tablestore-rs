use aliyun_tablestore_rs_macro::PerRequestOptions;
use prost::Message;

use crate::{protos::table_store::{DeleteDefinedColumnRequest, DeleteDefinedColumnResponse}, OtsClient, OtsOp, OtsRequest, OtsResult};

#[derive(Default, PerRequestOptions)]
pub struct DeleteDefinedColumnOperation {
    client: OtsClient,
    table_name: String,
    columns: Vec<String>,
}

impl DeleteDefinedColumnOperation {
    pub(crate) fn new(client: OtsClient, table_name: &str) -> Self {
        Self {
            client,
            table_name: table_name.to_string(),
            columns: Vec::new(),
        }
    }

    /// 添加要删除的列
    pub fn delete_column(mut self, col_name: &str) -> Self {
        self.columns.push(col_name.to_string());
        self
    }

    pub async fn send(self) -> OtsResult<DeleteDefinedColumnResponse> {
        let Self {
            client,
            table_name,
            columns,
        } = self;

        let msg = DeleteDefinedColumnRequest {
            table_name,
            columns,
        };

        let req = OtsRequest {
            operation: OtsOp::DeleteDefinedColumn,
            body: msg.encode_to_vec(),
            ..Default::default()
        };

        let response = client.send(req).await?;
        Ok(DeleteDefinedColumnResponse::decode(response.bytes().await?)?)
    }
}
