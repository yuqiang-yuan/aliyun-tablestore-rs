use prost::Message;

use crate::{add_per_request_options, protos::table_store::DropIndexRequest, OtsClient, OtsOp, OtsRequest, OtsResult};

/// 删除二级索引
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/createindex>
#[derive(Debug, Default, Clone)]
pub struct DropIndexOperation {
    client: OtsClient,
    request: DropIndexRequest,
}

add_per_request_options!(DropIndexOperation);

impl DropIndexOperation {
    pub(crate) fn new(client: OtsClient, request: DropIndexRequest) -> Self {
        Self {
            client,
            request
        }
    }

    pub async fn send(self) -> OtsResult<()> {
        let Self {
            client,
            request
        } = self;

        let req = OtsRequest {
            operation: OtsOp::DropIndex,
            body: request.encode_to_vec(),
            ..Default::default()
        };

        let res = client.send(req).await?;
        res.bytes().await?;

        Ok(())
    }
}
