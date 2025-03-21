use prost::Message;
use reqwest::Method;

use crate::{
    OtsClient, OtsOp, OtsRequest, OtsResult, add_per_request_options,
    error::OtsError,
    protos::table_store::{DeleteTableRequest, DeleteTableResponse},
};

use super::rules::validate_table_name;

/// 删除本实例下指定的表。
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/deletetable>
#[derive(Debug, Clone, Default)]
pub struct DeleteTableOperation {
    client: OtsClient,
    pub table_name: String,
}

add_per_request_options!(DeleteTableOperation);

impl DeleteTableOperation {
    pub(crate) fn new(client: OtsClient, table_name: &str) -> Self {
        Self {
            client,
            table_name: table_name.to_string(),
        }
    }
}

impl DeleteTableOperation {
    fn validate(&self) -> OtsResult<()> {
        if !validate_table_name(&self.table_name) {
            return Err(OtsError::ValidationFailed(format!("Invalid table name: {}", self.table_name)));
        }

        Ok(())
    }

    pub async fn send(self) -> OtsResult<DeleteTableResponse> {
        self.validate()?;

        let Self { client, table_name } = self;

        let msg = DeleteTableRequest { table_name };

        let req = OtsRequest {
            method: Method::POST,
            operation: OtsOp::DeleteTable,
            body: msg.encode_to_vec(),
            ..Default::default()
        };

        let response = client.send(req).await?;
        Ok(DeleteTableResponse::decode(response.bytes().await?)?)
    }
}
