//! 多元索引模块

mod list_search_index;
mod create_search_index;
mod describe_search_index;
mod update_search_index;
mod delete_search_index;

pub use list_search_index::*;
pub use create_search_index::*;
pub use describe_search_index::*;
pub use update_search_index::*;
pub use delete_search_index::*;

#[cfg(test)]
mod test_search_index {
    use crate::{protos::search::{CreateSearchIndexRequest, FieldSchema, FieldType, IndexSchema}, test_util::setup, OtsClient};

    #[tokio::test]
    async fn test_list_search_index() {
        setup();

        let client = OtsClient::from_env();
        let res = client.list_search_index(None).send().await;
        log::debug!("{:#?}", res);
    }

    #[tokio::test]
    async fn test_create_search_index() {
        setup();

        let client = OtsClient::from_env();
        let res = client.create_search_index(CreateSearchIndexRequest {
            table_name: "data_types".to_string(),
            index_name: "si_1".to_string(),
            schema: Some(IndexSchema {
                field_schemas: vec![
                    FieldSchema {
                        field_name: Some("str_col".to_string()),
                        field_type: Some(FieldType::Text as i32),
                        ..Default::default()
                    }
                ],
                index_setting: None,
                index_sort: None
            }),
            ..Default::default()
        }).send().await;

        log::debug!("{:#?}", res);

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_describe_search_index() {
        setup();

        let client = OtsClient::from_env();
        let res = client.describe_search_index("data_types", "si_1").send().await;
        log::debug!("{:#?}", res);

    }

    #[tokio::test]
    async fn test_delete_search_index() {
        setup();

        let client = OtsClient::from_env();
        let res = client.delete_search_index("data_types", "si_1").send().await;
        log::debug!("{:#?}", res);

    }
}
