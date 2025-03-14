use aliyun_tablestore_rs_macro::PerRequestOptions;
use prost::Message;
use reqwest::Method;

use crate::{
    OtsClient, OtsOp, OtsRequest, OtsResult,
    protos::table_store::{DeleteTableRequest, DeleteTableResponse},
};

/// Delete table
#[derive(Default, PerRequestOptions)]
pub struct DeleteTableOperation {
    client: OtsClient,
    table_name: String,
}

impl DeleteTableOperation {
    pub(crate) fn new(client: OtsClient, table_name: &str) -> Self {
        Self {
            client,
            table_name: table_name.to_string(),
        }
    }
}

impl DeleteTableOperation {
    pub async fn send(self) -> OtsResult<DeleteTableResponse> {
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
