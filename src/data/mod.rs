//! 宽表模型数据操作
mod get_range;
mod get_row;
mod put_row;
mod update_row;

pub use get_range::*;
pub use get_row::*;
pub use put_row::*;
pub use update_row::*;

#[cfg(test)]
mod test_row_operations {
    use std::sync::Once;

    use fake::{Fake, faker::name::raw::Name, locales::ZH_CN, uuid::UUIDv4};

    use crate::{
        OtsClient,
        model::{Column, ColumnValue, PrimaryKeyValue, Row, SingleColumnValueFilter},
        protos::table_store::{Direction, ReturnType},
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
            .primary_key_string("school_id", "00020FFB-BB14-CCAD-0181-A929E71C7312")
            .primary_key_integer("id", 1742203524276000)
            .max_versions(1)
            .send()
            .await;

        log::debug!("get data response: \n{:?}", response);
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
            .start_primary_key_string("cc_id", "0080669C-3A83-4B94-8D3A-C4A1FC54EBB1")
            .start_primary_key_string("stat_date", "2023-12-04")
            .start_primary_key_inf_min("user_id")
            .start_primary_key_inf_min("id")
            .end_primary_key_string("cc_id", "0082455B-D5A7-11E8-AF2C-7CD30AC4E9EA")
            .end_primary_key_string("stat_date", "2023-12-04")
            .end_primary_key_inf_max("user_id")
            .end_primary_key_inf_max("id")
            .filter(crate::model::Filter::Single(
                SingleColumnValueFilter::new()
                    .equal_column(Column::from_string("cc_school_id", "A006D67B-4330-1DEF-1354-0DB43F2F5F21"))
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
                op.inclusive_start_primary_keys = keys;
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

    async fn test_put_row_impl() {
        setup();

        let client = OtsClient::from_env();

        let school_id = UUIDv4.fake();

        let row = Row::default()
            .primary_key_string("school_id", &school_id)
            .primary_key_auto_increment("id")
            .column_string("name", Name(ZH_CN).fake::<String>())
            .column_string("province", Name(ZH_CN).fake::<String>());

        log::debug!("insert row into schools with school_id: {:?}", row.get_primary_key_value("school_id"));

        let response = client.put_row("schools").row(row).return_type(ReturnType::RtPk).send().await.unwrap();

        assert!(response.row.is_some());

        let row = response.row;
        assert!(row.is_some());

        let row = row.unwrap();
        assert_eq!(Some(&PrimaryKeyValue::String(school_id)), row.get_primary_key_value("school_id"));
    }

    #[tokio::test]
    async fn test_put_row() {
        test_put_row_impl().await;
    }

    async fn test_update_row_impl() {
        setup();
        let client = OtsClient::from_env();

        let table_name = "data_types";
        let id: String = UUIDv4.fake();

        log::debug!("insert new data to test update with id: {}", id);

        let response = client
            .put_row(table_name)
            .row(
                Row::new()
                    .primary_key_string("str_id", &id)
                    .column_string("str_col", "a")
                    .column_integer("int_col", 1)
                    .column_double("double_col", 1.234)
                    .column_bool("bool_col", false)
                    .column_blob("blob_col", b"a"),
            )
            .send()
            .await;

        assert!(response.is_ok());

        log::debug!("update row with id: {}", id);
        let response = client
            .update_row(table_name)
            .row(
                Row::new()
                    .primary_key_string("str_id", &id)
                    .column_string("str_col", "b")
                    .column_to_increse("int_col", 1)
                    .column_bool("bool_col", true)
                    .column_to_delete_all_versions("blob_col"),
            )
            .return_type(ReturnType::RtPk)
            .send()
            .await;

        assert!(response.is_ok());

        log::debug!("update row response: {:#?}", response);

        let response = client.get_row(table_name).primary_key_string("str_id", &id).send().await;

        assert!(response.is_ok());

        let response = response.unwrap();
        let row = response.row;
        assert!(row.is_some());

        let row = row.unwrap();
        assert_eq!(Some(&ColumnValue::String("b".to_string())), row.get_column_value("str_col"));
        assert_eq!(Some(&ColumnValue::Integer(2)), row.get_column_value("int_col"));
        assert_eq!(Some(&ColumnValue::Double(1.234)), row.get_column_value("double_col"));
        assert_eq!(Some(&ColumnValue::Boolean(true)), row.get_column_value("bool_col"));
        assert_eq!(None, row.get_column_value("blob_col"));
    }

    #[tokio::test]
    async fn test_update_row() {
        test_update_row_impl().await;
    }
}
