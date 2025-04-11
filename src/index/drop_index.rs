use prost::Message;

use crate::{add_per_request_options, protos::DropIndexRequest, OtsClient, OtsOp, OtsRequest, OtsResult};

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
    pub(crate) fn new(client: OtsClient, table_name: &str, idx_name: &str) -> Self {
        Self {
            client,
            request: DropIndexRequest {
                main_table_name: table_name.to_string(),
                index_name: idx_name.to_string(),
            },
        }
    }

    pub async fn send(self) -> OtsResult<()> {
        let Self { client, request } = self;

        let req = OtsRequest {
            operation: OtsOp::DropIndex,
            body: request.encode_to_vec(),
            ..Default::default()
        };

        let resp = client.send(req).await?;
        resp.bytes().await?;

        Ok(())
    }
}
