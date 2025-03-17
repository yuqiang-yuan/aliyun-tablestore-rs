//! 基础数据操作
mod get_range;
mod get_row;

pub use get_range::*;
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
        let response = client
            .get_row("schools")
            .add_string_pk_value("school_id", "00020FFB-BB14-CCAD-0181-A929E71C7312")
            .add_integer_pk_value("id", 1742203524276000)
            .max_versions(21)
            .send()
            .await;

        log::debug!("get row response: \n{:#?}", response);
        assert!(response.is_ok());

        // let response = response.unwrap();
        // std::fs::write("/home/yuanyq/Downloads/aliyun-plainbuffer/get-row-response-versions.data", response.row).unwrap();
        // let response = client.get_row("users")
        //     .add_string_pk_value("user_id", "0005358A-DCAF-665E-EECF-D9935E821B87")
        //     .max_versions(1)
        //     .send().await;

        // log::debug!("get row response: \n{:#?}", response);
        // assert!(response.is_ok());

        // let response = response.unwrap();
        // std::fs::write("/home/yuanyq/Downloads/aliyun-plainbuffer/get-row-response.data", response.row).unwrap();
    }

    #[tokio::test]
    async fn test_get_row() {
        test_get_row_impl().await;
    }

    async fn test_get_range_impl() {
        setup();
        let client = OtsClient::from_env();

        let response = client.get_range("").send().await.unwrap();
        std::fs::write("/home/yuanyq/Downloads/aliyun-plainbuffer/get-range-response.data", response.rows).unwrap();
    }

    #[tokio::test]
    async fn test_get_range() {
        test_get_range_impl().await;
    }
}
