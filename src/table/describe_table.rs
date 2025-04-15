use prost::Message;
use reqwest::Method;

use crate::{
    add_per_request_options,
    error::OtsError,
    protos::{DescribeTableRequest, DescribeTableResponse},
    OtsClient, OtsOp, OtsRequest, OtsResult,
};

use crate::model::rules::validate_table_name;

/// 查询指定表的结构信息以及预留读吞吐量和预留写吞吐量设置信息。
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/describetable>
#[derive(Default, Debug, Clone)]
pub struct DescribeTableOperation {
    client: OtsClient,
    table_name: String,
}

add_per_request_options!(DescribeTableOperation);

impl DescribeTableOperation {
    pub(crate) fn new(client: OtsClient, table_name: &str) -> Self {
        Self {
            client,
            table_name: table_name.to_string(),
        }
    }

    fn validate(&self) -> OtsResult<()> {
        if !validate_table_name(&self.table_name) {
            return Err(OtsError::ValidationFailed(format!("Invalid table name: {}", self.table_name)));
        }

        Ok(())
    }

    pub async fn send(self) -> OtsResult<DescribeTableResponse> {
        self.validate()?;

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
