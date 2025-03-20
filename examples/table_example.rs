use aliyun_tablestore_rs::{OtsClient, model::ColumnValue};
use dotenvy::dotenv;

pub fn main() {
    dotenv().unwrap();
    let client = OtsClient::from_env();
    let _op = client.get_range("table_name");
    println!("{:#?}", client);

    let _ = ColumnValue::Null;
}
