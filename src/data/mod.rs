//! 宽表模型数据操作
mod get_range;
mod get_row;
mod put_row;

pub use get_range::*;
pub use get_row::*;
pub use put_row::*;

#[cfg(test)]
mod test_row {
    use std::sync::Once;

    use crate::{
        OtsClient,
        model::{Column, ColumnValue, PrimaryKeyValue, SingleColumnValueFilter},
        protos::table_store::Direction,
    };

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

        log::debug!("get data response: \n{:#?}", response);
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.row.is_some());
        assert!(
            response
                .row
                .as_ref()
                .unwrap()
                .primary_keys
                .iter()
                .any(|k| { &k.name == "school_id" && k.value == PrimaryKeyValue::String("00020FFB-BB14-CCAD-0181-A929E71C7312".to_string()) })
        );

        // let response = response.unwrap();
        // std::fs::write("/home/yuanyq/Downloads/aliyun-plainbuffer/get-data-response-versions.data", response.data).unwrap();
        // let response = client.get_row("users")
        //     .add_string_pk_value("user_id", "0005358A-DCAF-665E-EECF-D9935E821B87")
        //     .max_versions(1)
        //     .send().await;

        // log::debug!("get data response: \n{:#?}", response);
        // assert!(response.is_ok());

        // let response = response.unwrap();
        // std::fs::write("/home/yuanyq/Downloads/aliyun-plainbuffer/get-data-response.data", response.data).unwrap();
    }

    #[tokio::test]
    async fn test_get_row() {
        test_get_row_impl().await;
    }

    async fn test_get_range_with_single_filter_impl() {
        setup();
        let client = OtsClient::from_env();

        // let response = client.get_range("schools")
        //     .add_inf_min_start_pk_value("school_id")
        //     .add_inf_min_start_pk_value("id")
        //     .add_inf_max_end_pk_value("school_id")
        //     .add_inf_max_end_pk_value("id")
        //     .max_versions(1)
        //     .limit(1000)
        //     .direction(Direction::Forward)
        //     .send().await;

        // let mut op = client
        //     .get_range("ccNgMemberRecord")
        //     .add_inf_min_start_pk_value("cc_id")
        //     .add_string_start_pk_value("stat_date", "2023-12-04")
        //     .add_inf_min_start_pk_value("user_id")
        //     .add_inf_min_start_pk_value("id")
        //     .add_inf_max_end_pk_value("cc_id")
        //     .add_string_end_pk_value("stat_date", "2023-12-04")
        //     .add_inf_max_end_pk_value("user_id")
        //     .add_inf_max_end_pk_value("id")
        //     .max_versions(1)
        //     .limit(1000)
        //     .direction(Direction::Forward);

        let mut op = client
            .get_range("ccNgMemberRecord")
            .add_string_start_pk_value("cc_id", "0080669C-3A83-4B94-8D3A-C4A1FC54EBB1")
            .add_string_start_pk_value("stat_date", "2023-12-04")
            .add_inf_min_start_pk_value("user_id")
            .add_inf_min_start_pk_value("id")
            .add_string_end_pk_value("cc_id", "0082455B-D5A7-11E8-AF2C-7CD30AC4E9EA")
            .add_string_end_pk_value("stat_date", "2023-12-04")
            .add_inf_max_end_pk_value("user_id")
            .add_inf_max_end_pk_value("id")
            .filter(crate::model::Filter::Single(
                SingleColumnValueFilter::new()
                    .equal_column(Column::with_string_value("cc_school_id", "A006D67B-4330-1DEF-1354-0DB43F2F5F21"))
                    .filter_if_missing(false)
                    .latest_version_only(true),
            ))
            .max_versions(1)
            .limit(1000)
            .direction(Direction::Forward);

        let mut total_row = 0;

        loop {
            let response = op.clone().send().await;

            assert!(response.is_ok());
            let response = response.unwrap();

            for row in &response.rows {
                log::debug!(
                    "cc_id: {:?}, user_id: {:?}, school_id: {:?}",
                    row.get_primary_key_value("cc_id"),
                    row.get_primary_key_value("user_id"),
                    row.get_column_value("cc_school_id")
                );

                assert_eq!(
                    Some(&ColumnValue::String("A006D67B-4330-1DEF-1354-0DB43F2F5F21".to_string())),
                    row.get_column_value("cc_school_id")
                );
            }

            total_row += response.rows.len();
            log::debug!("total read: {} rows", total_row);

            if let Some(keys) = response.next_start_primary_key {
                log::debug!("Going to send next query");
                op.inclusive_start_primary_key = keys;
            } else {
                break;
            }
        }
        // log::debug!("{:#?}", response);
        // assert_eq!(2, response.rows.len());
    }

    #[tokio::test]
    async fn test_get_range_with_single_filter() {
        test_get_range_with_single_filter_impl().await;
    }
}
