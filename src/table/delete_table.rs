use prost::Message;
use reqwest::Method;

use crate::{
    OtsClient, OtsOp, OtsRequest, OtsResult, add_per_request_options,
    error::OtsError
};

use super::rules::validate_table_name;

#[derive(Debug, Default, Clone)]
pub struct DeleteTableRequest {
    pub table_name: String,
}

impl DeleteTableRequest {
    pub fn new(table_name: &str) -> Self {
        Self {
            table_name: table_name.to_string()
        }
    }

    fn validate(&self) -> OtsResult<()> {
        if !validate_table_name(&self.table_name) {
            return Err(OtsError::ValidationFailed(format!("Invalid table name: {}", self.table_name)));
        }

        Ok(())
    }
}

impl From<DeleteTableRequest> for crate::protos::table_store::DeleteTableRequest {
    fn from(value: DeleteTableRequest) -> Self {
        crate::protos::table_store::DeleteTableRequest {
            table_name: value.table_name
        }
    }
}

/// 删除本实例下指定的表。
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/deletetable>
#[derive(Debug, Clone, Default)]
pub struct DeleteTableOperation {
    client: OtsClient,
    request: DeleteTableRequest,
}

add_per_request_options!(DeleteTableOperation);

impl DeleteTableOperation {
    pub(crate) fn new(client: OtsClient, table_name: &str) -> Self {
        Self {
            client,
            request: DeleteTableRequest::new(table_name)
        }
    }
}

impl DeleteTableOperation {
    pub async fn send(self) -> OtsResult<()> {
        self.request.validate()?;

        let Self { client, request } = self;

        let msg: crate::protos::table_store::DeleteTableRequest = request.into();

        let req = OtsRequest {
            method: Method::POST,
            operation: OtsOp::DeleteTable,
            body: msg.encode_to_vec(),
            ..Default::default()
        };

        let response = client.send(req).await?;
        response.bytes().await?;

        Ok(())
    }
}
