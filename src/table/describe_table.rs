use aliyun_tablestore_rs_macro::PerRequestOptions;
use prost::Message;
use reqwest::Method;

use crate::{
    OtsClient, OtsOp, OtsRequest, OtsResult,
    protos::table_store::{DescribeTableRequest, DescribeTableResponse},
};

/// Describe table
#[derive(Default, PerRequestOptions)]
pub struct DescribeTableOperation {
    client: OtsClient,
    table_name: String,
}

impl DescribeTableOperation {
    pub(crate) fn new(client: OtsClient, table_name: &str) -> Self {
        Self {
            client,
            table_name: table_name.to_string(),
        }
    }

    pub async fn send(self) -> OtsResult<DescribeTableResponse> {
        let Self { client, table_name } = self;

        let body = DescribeTableRequest { table_name }.encode_to_vec();

        let req = OtsRequest {
            method: Method::POST,
            operation: OtsOp::DescribeTable,
            body,
            ..Default::default()
        };

        let response = client.send(req).await?;

        Ok(DescribeTableResponse::decode(response.bytes().await?)?)
    }
}
