use prost::Message;

use crate::{add_per_request_options, error::OtsError, timeseries_model::rules::validate_timeseries_table_name, OtsClient, OtsOp, OtsRequest, OtsResult};

/// 删除指定时序表
pub struct DeleteTimeseriesTableOperation {
    client: OtsClient,
    table_name: String,
}

add_per_request_options!(DeleteTimeseriesTableOperation);

impl DeleteTimeseriesTableOperation {
    pub(crate) fn new(client: OtsClient, table_name: &str) -> Self {
        Self {
            client,
            table_name: table_name.to_string(),
        }
    }

    pub async fn send(self) -> OtsResult<()> {
        if !validate_timeseries_table_name(&self.table_name) {
            return Err(OtsError::ValidationFailed(format!("invalid table name: {}", self.table_name)));
        }

        let Self { client, table_name } = self;
        let msg = crate::protos::timeseries::DeleteTimeseriesTableRequest { table_name };

        let req = OtsRequest {
            operation: OtsOp::DeleteTimeseriesTable,
            body: msg.encode_to_vec(),
            ..Default::default()
        };

        let resp = client.send(req).await?;
        resp.bytes().await?;

        Ok(())
    }
}
