use prost::Message;

use crate::{OtsClient, OtsOp, OtsRequest, OtsResult, add_per_request_options, protos::CreateIndexRequest};

/// 创建二级索引。仅 `max_versions = 1` 的表可以创建二级索引
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/createindex>
#[derive(Debug, Clone, Default)]
pub struct CreateIndexOperation {
    client: OtsClient,
    request: CreateIndexRequest,
}

add_per_request_options!(CreateIndexOperation);

impl CreateIndexOperation {
    pub(crate) fn new(client: OtsClient, request: CreateIndexRequest) -> Self {
        Self { client, request }
    }

    pub async fn send(self) -> OtsResult<()> {
        let Self { client, request } = self;

        let req = OtsRequest {
            operation: OtsOp::CreateIndex,
            body: request.encode_to_vec(),
            ..Default::default()
        };

        let res = client.send(req).await?;
        res.bytes().await?;

        Ok(())
    }
}
