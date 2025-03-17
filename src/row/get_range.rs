use prost::Message;

use crate::{
    OtsClient, OtsOp, OtsRequest, OtsResult, add_per_request_options,
    model::{PrimaryKey, PrimaryKeyColumn, PrimaryKeyValue},
    protos::table_store::{Direction, GetRangeRequest, GetRangeResponse},
};

#[derive(Default)]
pub struct GetRangeOperation {
    client: OtsClient,
    inclusive_start_primary_key: Vec<PrimaryKeyColumn>,
    exclusive_end_primary_key: Vec<PrimaryKeyColumn>,
    max_version: Option<i32>,
}

add_per_request_options!(GetRangeOperation);

impl GetRangeOperation {
    pub(crate) fn new(client: OtsClient, table_name: &str) -> Self {
        Self { client, ..Default::default() }
    }

    pub async fn send(self) -> OtsResult<GetRangeResponse> {
        let Self {
            client,
            inclusive_start_primary_key,
            exclusive_end_primary_key,
            max_version,
        } = self;

        let pk1 = vec![
            PrimaryKeyColumn {
                name: "school_id".to_string(),
                value: PrimaryKeyValue::InfMin,
                ..Default::default()
            },
            PrimaryKeyColumn {
                name: "id".to_string(),
                value: PrimaryKeyValue::InfMin,
                ..Default::default()
            },
        ];

        let pk2 = vec![
            PrimaryKeyColumn {
                name: "school_id".to_string(),
                value: PrimaryKeyValue::InfMax,
                ..Default::default()
            },
            PrimaryKeyColumn {
                name: "id".to_string(),
                value: PrimaryKeyValue::InfMax,
                ..Default::default()
            },
        ];

        let msg = GetRangeRequest {
            table_name: "schools".to_string(),
            direction: Direction::Forward as i32,
            columns_to_get: vec![],
            time_range: None,
            max_versions: Some(1),
            limit: Some(100),
            inclusive_start_primary_key: (PrimaryKey { keys: pk1 }).into_plain_buffer(true),
            exclusive_end_primary_key: (PrimaryKey { keys: pk2 }).into_plain_buffer(true),
            filter: None,
            start_column: None,
            end_column: None,
            token: None,
            transaction_id: None,
        };

        let req = OtsRequest {
            operation: OtsOp::GetRange,
            body: msg.encode_to_vec(),
            ..Default::default()
        };

        let response = client.send(req).await?;

        Ok(GetRangeResponse::decode(response.bytes().await?)?)
    }
}
