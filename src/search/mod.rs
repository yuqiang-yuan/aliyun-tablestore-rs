//! 多元索引模块

mod list_search_index;
mod create_search_index;
mod describe_search_index;
mod update_search_index;
mod delete_search_index;
mod search;
mod filter;
mod aggregation;
mod group_by;
mod sort_by;
mod query;

pub use list_search_index::*;
pub use create_search_index::*;
pub use describe_search_index::*;
use regex::Regex;
pub use update_search_index::*;
pub use delete_search_index::*;
pub use search::*;
pub use filter::*;
pub use aggregation::*;
pub use group_by::*;
pub use sort_by::*;
pub use query::*;

/// 验证分组名称是否符合规范
///
/// 分组名称应符合以下规范：
///
/// - 由英文字母、数字或下划线组成
/// - 大小写敏感
/// - 长度为 1~128 个字符
/// - 首字母必须为英文字母或下划线
///
/// # Arguments
///
/// * `name` - 要验证的索引名称
///
/// # Returns
/// * `true` - 名称符合规范
/// * `false` - 名称不符合规范
pub(crate) fn validate_group_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 128 {
        return false;
    }

    let first_char = name.chars().next().unwrap();
    if !first_char.is_ascii_alphabetic() && first_char != '_' {
        return false;
    }

    name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// 验证排序名称
pub(crate) fn validate_sort_name(name: &str) -> bool {
    validate_group_name(name)
}

/// 验证聚合名称
pub(crate) fn validate_aggregation_name(name: &str) -> bool {
    validate_group_name(name)
}

/// 验证是否是符合 OTS 要求的时区字符串
pub(crate) fn validate_timezone_string(tz: &str) -> bool {
    let regex = Regex::new(r"(?m)^[+-]\d{2}:\d{2}$").unwrap();
    regex.is_match(tz)
}

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
