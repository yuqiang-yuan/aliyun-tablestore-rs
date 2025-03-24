//! 宽表操作
mod compute_split_points;
mod create_table;
mod delete_table;
mod describe_table;
mod list_table;
mod update_table;

pub use compute_split_points::*;
pub use create_table::*;
pub use delete_table::*;
pub use describe_table::*;
pub use list_table::*;
pub use update_table::*;

/// Validation rules for table
pub(crate) mod rules {
    /// 一个宽表至少有 1 个主键列
    pub const MIN_PRIMARY_KEY_COUNT: usize = 1;

    /// 一个宽表最多 4 个主键列
    pub const MAX_PRIMARY_KEY_COUNT: usize = 4;

    /// 约束条件：
    ///
    /// - 由英文字母、数字或下划线（_）组成，大小写敏感，长度限制为1~255字节。
    /// - 首字母必须为英文字母或下划线（_）。
    pub fn validate_table_name(table_name: &str) -> bool {
        if table_name.is_empty() || table_name.len() > 255 {
            return false;
        }

        let first_char = match table_name.chars().next() {
            Some(c) => c,
            None => return false,
        };

        if !first_char.is_ascii_alphabetic() && first_char != '_' {
            return false;
        }

        table_name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    }

    /// 和表名的约束条件一样
    pub fn validate_column_name(col_name: &str) -> bool {
        validate_table_name(col_name)
    }

    pub fn validate_index_name(idx_name: &str) -> bool {
        validate_table_name(idx_name)
    }
}

#[cfg(test)]
mod test_table {
    use std::sync::Once;

    use crate::{
        OtsClient,
        index::IndexMetaBuilder,
        protos::table_store::IndexType,
        table::{CreateTableRequest, UpdateTableRequest},
    };

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::dotenv().unwrap();
        });
    }

    #[tokio::test]
    async fn test_list_table() {
        setup();

        let client = OtsClient::from_env();
        let list_table_response = client.list_table().send().await;
        log::debug!("{:#?}", list_table_response);
        assert!(list_table_response.is_ok());
        let tables = list_table_response.unwrap().table_names;
        assert!(tables.len() > 0);
    }

    #[tokio::test]
    async fn test_desc_table() {
        setup();
        let client = OtsClient::from_env();

        let desc_response = client.describe_table("users").send().await;
        log::debug!("describe table users: {:#?}", desc_response);
        assert!(desc_response.is_ok());

        let info = desc_response.unwrap();
        let pk = &info.table_meta.primary_key;
        assert_eq!(1, pk.len());
        assert_eq!("user_id", &pk.get(0).unwrap().name);
    }

    async fn test_create_table_impl() {
        setup();
        let client = OtsClient::from_env();

        let req = CreateTableRequest::new("users1")
            .primary_key_string("user_id_part")
            .primary_key_string("user_id")
            .column_string("full_name")
            .column_string("phone_number")
            .column_string("pwd_hash")
            .column_string("badge_no")
            .column_string("gender")
            .column_integer("registered_at_ms")
            .column_bool("deleted")
            .column_integer("deleted_at_ms")
            .column_double("score")
            .column_blob("avatar")
            .index(
                IndexMetaBuilder::new("idx_phone_no1")
                    .primary_key("user_id_part")
                    .defined_column("phone_number")
                    .index_type(IndexType::ItGlobalIndex)
                    .build(),
            );

        // let msg: crate::protos::table_store::CreateTableRequest = req.into();
        // let bytes = msg.encode_to_vec();

        // std::fs::write("/home/yuanyq/Downloads/aliyun-plainbuffer/create-table.data", &bytes).unwrap();

        // let msg = crate::protos::table_store::CreateTableRequest::decode(bytes.as_slice());
        // log::debug!("{:#?}", msg);

        let response = client.create_table(req).send().await;

        log::debug!("{:#?}", response);

        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_create_table() {
        test_create_table_impl().await;
    }

    async fn test_validate_create_table_impl() {
        setup();
        let client = OtsClient::from_env();
        let response = client
            .create_table(CreateTableRequest::new("_invalid_table_name"))
            .timeout_ms(1000)
            .send()
            .await;
        assert!(response.is_err());

        let response = client.create_table(CreateTableRequest::new("1dd")).send().await;
        assert!(response.is_err());

        let response = client.create_table(CreateTableRequest::new("a,b")).send().await;
        assert!(response.is_err());

        let response = client.create_table(CreateTableRequest::new("中文")).send().await;
        assert!(response.is_err());

        let response = client.create_table(CreateTableRequest::new("validname").primary_key_string("1")).send().await;

        assert!(response.is_err());
    }

    #[tokio::test]
    async fn test_validate_create_table() {
        test_validate_create_table_impl().await;
    }

    async fn test_update_table_impl() {
        setup();
        let client = OtsClient::from_env();
        let response = client
            .update_table(UpdateTableRequest::new("ccs").reserved_throughput_read(0).reserved_throughput_write(0))
            .send()
            .await;

        log::debug!("{:#?}", response);
        assert!(response.is_ok());

        let response = response.unwrap();
        assert_eq!(Some(0), response.reserved_throughput_details.capacity_unit.read);
        assert_eq!(Some(0), response.reserved_throughput_details.capacity_unit.write);
    }

    #[tokio::test]
    async fn test_update_table() {
        test_update_table_impl().await;
    }

    async fn test_delete_table_impl() {
        setup();
        let client = OtsClient::from_env();
        let response = client.delete_table("users1").send().await;

        log::debug!("{:#?}", response);
        assert!(response.is_ok());

        let tables = client.list_table().send().await.unwrap().table_names;
        assert!(!tables.contains(&"ccs1".to_string()));
    }

    #[tokio::test]
    async fn test_delete_table() {
        test_delete_table_impl().await;
    }

    async fn test_retry_impl() {
        setup();
        let client = OtsClient::from_env();

        for i in 0..100 {
            let _ = client.list_table().send().await;
            log::debug!("list table to test retry, round: {}", i + 1);
        }
    }

    #[tokio::test]
    async fn test_retry() {
        test_retry_impl().await;
    }
}
