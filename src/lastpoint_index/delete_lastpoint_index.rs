use prost::Message;

use crate::{
    OtsClient, OtsOp, OtsRequest, OtsResult, add_per_request_options,
    error::OtsError,
    timeseries_model::rules::{validate_lastpoint_index_name, validate_timeseries_table_name},
};

/// 删除 lastpoint 索引
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/deletetimeserieslastpointindex>
#[derive(Debug, Default, Clone)]
pub struct DeleteTimeseriesLastpointIndexOperation {
    client: OtsClient,
    table_name: String,
    index_name: String,
}

add_per_request_options!(DeleteTimeseriesLastpointIndexOperation);

impl DeleteTimeseriesLastpointIndexOperation {
    pub(crate) fn new(client: OtsClient, table_name: &str, index_name: &str) -> Self {
        Self {
            client,
            table_name: table_name.to_string(),
            index_name: index_name.to_string(),
        }
    }

    pub async fn send(self) -> OtsResult<()> {
        if !validate_timeseries_table_name(&self.table_name) {
            return Err(OtsError::ValidationFailed(format!("invalid table name: {}", self.table_name)));
        }

        if !validate_lastpoint_index_name(&self.index_name) {
            return Err(OtsError::ValidationFailed(format!(
                "invalid timeseries table lastpoint index name: {}",
                self.index_name
            )));
        }

        let Self {
            client,
            table_name,
            index_name,
        } = self;

        let msg = crate::protos::timeseries::DeleteTimeseriesLastpointIndexRequest {
            main_table_name: table_name,
            index_table_name: index_name,
        };

        let req = OtsRequest {
            operation: OtsOp::DeleteTimeseriesLastpointIndex,
            body: msg.encode_to_vec(),
            ..Default::default()
        };

        let resp = client.send(req).await?;
        resp.bytes().await?;

        Ok(())
    }
}
