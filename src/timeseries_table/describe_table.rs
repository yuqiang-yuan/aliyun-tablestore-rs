use prost::Message;

use crate::{
    add_per_request_options, error::OtsError, protos::timeseries::DescribeTimeseriesTableResponse, timeseries_model::rules::validate_timeseries_table_name,
    OtsClient, OtsOp, OtsRequest, OtsRequestOptions, OtsResult,
};

/// 获取时序表信息
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/describetimeseriestable>
#[derive(Clone)]
pub struct DescribeTimeseriesTableOperation {
    client: OtsClient,
    table_name: String,
    options: OtsRequestOptions,
}

add_per_request_options!(DescribeTimeseriesTableOperation);

impl DescribeTimeseriesTableOperation {
    pub(crate) fn new(client: OtsClient, table_name: &str) -> Self {
        Self {
            client,
            table_name: table_name.to_string(),
            options: OtsRequestOptions::default(),
        }
    }

    pub async fn send(self) -> OtsResult<DescribeTimeseriesTableResponse> {
        if !validate_timeseries_table_name(&self.table_name) {
            return Err(OtsError::ValidationFailed(format!("invalid table name: {}", self.table_name)));
        }

        let Self { client, table_name, options } = self;
        let msg = crate::protos::timeseries::DescribeTimeseriesTableRequest { table_name };

        let req = OtsRequest {
            operation: OtsOp::DescribeTimeseriesTable,
            body: msg.encode_to_vec(),
            options,
            ..Default::default()
        };

        let resp = client.send(req).await?;

        Ok(DescribeTimeseriesTableResponse::decode(resp.bytes().await?)?)
    }
}
