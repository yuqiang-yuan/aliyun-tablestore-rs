use prost::Message;
use reqwest::Method;

use crate::{
    add_per_request_options, protos::table_store::{DeleteTableRequest, DeleteTableResponse}, OtsClient, OtsOp, OtsRequest, OtsResult
};

/// Delete table
#[derive(Default)]
pub struct DeleteTableOperation {
    client: OtsClient,
    table_name: String,
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
