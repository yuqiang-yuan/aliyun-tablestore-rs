use prost::Message;

use crate::{add_per_request_options, OtsClient, OtsOp, OtsRequest, OtsResult};

#[derive(Debug, Default, Clone)]
pub struct ListTimeseriesTableOperation {
    client: OtsClient,
}

add_per_request_options!(ListTimeseriesTableOperation);

impl ListTimeseriesTableOperation {
    pub(crate) fn new(client: OtsClient) -> Self {
        Self { client }
    }

    pub async fn send(self) -> OtsResult<crate::protos::timeseries::ListTimeseriesTableResponse> {
        let req = OtsRequest {
            operation: OtsOp::ListTimeseriesTable,
            body: vec![],
            ..Default::default()
        };

        let resp = self.client.send(req).await?;
        let resp_msg = crate::protos::timeseries::ListTimeseriesTableResponse::decode(resp.bytes().await?)?;

        Ok(resp_msg)
    }
}
