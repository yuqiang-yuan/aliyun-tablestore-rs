use prost::Message;
use reqwest::Method;

use crate::{
    OtsClient, OtsOp, OtsRequest, OtsResult, add_per_request_options,
    protos::{ListTableRequest, ListTableResponse},
};

/// 获取当前实例下已创建的所有表的表名。
#[derive(Default, Debug, Clone)]
pub struct ListTableOperation {
    client: OtsClient,
}

add_per_request_options!(ListTableOperation);

impl ListTableOperation {
    pub(crate) fn new(client: OtsClient) -> Self {
        Self { client }
    }

    /// Consume the builder and send request
    pub async fn send(self) -> OtsResult<Vec<String>> {
        let msg = ListTableRequest {};
        let req = OtsRequest {
            method: Method::POST,
            operation: OtsOp::ListTable,
            body: msg.encode_to_vec(),
            ..Default::default()
        };

        let Self { client } = self;

        let response = client.send(req).await?;
        Ok(ListTableResponse::decode(response.bytes().await?)?.table_names)
    }
}
