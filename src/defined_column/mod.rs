//! 预定义列操作

mod add_defined_column;
mod delete_defined_column;

pub use add_defined_column::*;
pub use delete_defined_column::*;

#[cfg(test)]
mod test_defined_column {

    use std::sync::Once;

    use crate::OtsClient;

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::dotenv().unwrap();
        });
    }

    async fn test_add_defined_column_impl() {
        setup();
        let client = OtsClient::from_env();
        let response = client
            .add_defined_column("ccs")
            .add_integer_column("created_at")
            .add_string_column("cover_url")
            .add_double_column("avg_score")
            .send()
            .await;

        assert!(response.is_ok());

        let response = client.describe_table("ccs").send().await.unwrap();
        assert_eq!(6, response.table_meta.defined_column.len());
    }

    #[tokio::test]
    async fn test_add_defined_column() {
        test_add_defined_column_impl().await;
    }

    async fn test_delete_defined_column_impl() {
        setup();
        let client = OtsClient::from_env();
        let response = client.delete_defined_column("ccs").delete_column("created_at").send().await;

        log::debug!("{:#?}", response);
        assert!(response.is_ok());

        let response = client.describe_table("ccs").send().await.unwrap();
        assert_eq!(5, response.table_meta.defined_column.len());
    }

    #[tokio::test]
    async fn test_delete_defined_column() {
        test_delete_defined_column_impl().await;
    }
}
