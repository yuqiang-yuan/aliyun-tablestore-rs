use prost::Message;

use crate::{
    add_per_request_options,
    protos::search::{IndexInfo, ListSearchIndexRequest, ListSearchIndexResponse},
    OtsClient, OtsOp, OtsRequest, OtsResult,
};

/// 列出多元索引列表。
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/listsearchindex>
pub struct ListSearchIndexOperation {
    client: OtsClient,
    request: ListSearchIndexRequest,
}

add_per_request_options!(ListSearchIndexOperation);

impl ListSearchIndexOperation {
    pub(crate) fn new(client: OtsClient, table_name: Option<&str>) -> Self {
        Self {
            client,
            request: ListSearchIndexRequest {
                table_name: table_name.map(|s| s.into()),
            },
        }
    }

    pub async fn send(self) -> OtsResult<Vec<IndexInfo>> {
        let Self { client, request } = self;

        let req = OtsRequest {
            operation: OtsOp::ListSearchIndex,
            body: request.encode_to_vec(),
            ..Default::default()
        };

        let resp = client.send(req).await?;
        let resp_msg = ListSearchIndexResponse::decode(resp.bytes().await?)?;

        Ok(resp_msg.indices)
    }
}
