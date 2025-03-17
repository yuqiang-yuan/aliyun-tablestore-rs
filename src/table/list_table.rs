use prost::Message;
use reqwest::Method;

use crate::{
    add_per_request_options, protos::table_store::{ListTableRequest, ListTableResponse}, OtsClient, OtsOp, OtsRequest, OtsResult
};

/// List table
#[derive(Default)]
pub struct ListTableOperation {
    client: OtsClient,
}

add_per_request_options!(ListTableOperation);

impl ListTableOperation {
    pub(crate) fn new(client: OtsClient) -> Self {
        Self { client }
    }

    /// Consume the builder and send request
    pub async fn send(self) -> OtsResult<ListTableResponse> {
        let msg = ListTableRequest {};
        let req = OtsRequest {
            method: Method::POST,
            operation: OtsOp::ListTable,
            body: msg.encode_to_vec(),
            ..Default::default()
        };

        let Self { client } = self;

        let response = client.send(req).await?;
        Ok(ListTableResponse::decode(response.bytes().await?)?)
    }
}
