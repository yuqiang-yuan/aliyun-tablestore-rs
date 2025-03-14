//! 基础数据操作
mod get_row;

pub use get_row::*;

#[cfg(test)]
mod test_row {
    use std::sync::Once;

    use crate::OtsClient;

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::dotenv().unwrap();
        });
    }


    async fn test_get_row_impl() {
        setup();
        let client = OtsClient::from_env();
        let response = client.get_row("users")
            .add_string_pk_value("user_id", "0005358A-DCAF-665E-EECF-D9935E821B87")
            .max_versions(1)
            .send().await;

        log::debug!("get row response: \n{:#?}", response);
        assert!(response.is_ok());

        let response = response.unwrap();
    }

    #[tokio::test]
    async fn test_get_row() {
        test_get_row_impl().await;
    }
}
