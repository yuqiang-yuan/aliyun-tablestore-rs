use prost::Message;
use reqwest::Method;

use crate::{add_per_request_options, protos::table_store::{ComputeSplitPointsBySizeRequest, ComputeSplitPointsBySizeResponse}, OtsClient, OtsOp, OtsRequest, OtsResult};

#[derive(Default)]
pub struct ComputeSplitPointsBySizeOperation {
    client: OtsClient,
    table_name: String,
    split_size: u64,
    split_size_unit_in_byte: Option<u64>,
    split_point_limit: Option<u32>
}

add_per_request_options!(ComputeSplitPointsBySizeOperation);

impl ComputeSplitPointsBySizeOperation {
    pub(crate) fn new(client: OtsClient, table_name: &str, split_size: u64) -> Self {
        Self {
            client,
            table_name: table_name.to_string(),
            split_size,
            split_size_unit_in_byte: None,
            split_point_limit: None
        }
    }

    /// 每个分片的近似大小，以百兆为单位。
    pub fn split_size(mut self, split_size: u64) -> Self {
        self.split_size = split_size;
        self
    }

    /// 指定分割大小的单位，以便在分割点计算时使用正确的单位，并确保计算结果的准确性。
    pub fn split_size_unit_in_byte(mut self, split_size_unit_in_byte: u64) -> Self {
        self.split_size_unit_in_byte = Some(split_size_unit_in_byte);
        self
    }

    /// 指定对分割点数量的限制，以便在进行分割点计算时控制返回的分割点数量。
    pub fn split_point_limit(mut self, split_point_limit: u32) -> Self {
        self.split_point_limit = Some(split_point_limit);
        self
    }

    pub async fn send(self) -> OtsResult<ComputeSplitPointsBySizeResponse> {
        let Self {
            client,
            table_name,
            split_size,
            split_size_unit_in_byte,
            split_point_limit,
        } = self;

        let msg = ComputeSplitPointsBySizeRequest {
            table_name,
            split_size: split_size as i64,
            split_size_unit_in_byte: split_size_unit_in_byte.map(|n| n as i64),
            split_point_limit: split_point_limit.map(|n| n as i32),
        };

        let body = msg.encode_to_vec();

        let req = OtsRequest {
            method: Method::POST,
            operation: OtsOp::ComputeSplitPointsBySize,
            body,
            ..Default::default()
        };

        let response = client.send(req).await?;
        Ok(ComputeSplitPointsBySizeResponse::decode(response.bytes().await?)?)
    }
}
