use prost::Message;

use crate::{OtsClient, OtsOp, OtsRequest, OtsResult, add_per_request_options, protos::search::CreateSearchIndexRequest};

/// 接口创建一个多元索引。
///
/// 创建多元索引前，请确保数据表的最大版本数为 `1`，数据生命周期满足如下条件中的任意一个。
///
/// - 数据表的数据生命周期为 `-1`（数据永不过期）。
/// - 数据表的数据生命周期不为 `-1` 时，数据表为禁止更新状态（即是否允许更新为否）。
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/createsearchindex>
#[derive(Debug, Default, Clone)]
pub struct CreateSearchIndexOperation {
    client: OtsClient,
    request: CreateSearchIndexRequest,
}

add_per_request_options!(CreateSearchIndexOperation);

impl CreateSearchIndexOperation {
    pub(crate) fn new(client: OtsClient, request: CreateSearchIndexRequest) -> Self {
        Self { client, request }
    }

    pub async fn send(self) -> OtsResult<()> {
        let Self { client, request } = self;

        let req = OtsRequest {
            operation: OtsOp::CreateSearchIndex,
            body: request.encode_to_vec(),
            ..Default::default()
        };

        let res = client.send(req).await?;
        res.bytes().await?;
        Ok(())
    }
}
