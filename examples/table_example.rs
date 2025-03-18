use aliyun_tablestore_rs::OtsClient;
use dotenvy::dotenv;

pub fn main() {
    dotenv().unwrap();
    let client = OtsClient::from_env();
    println!("{:#?}", client);
}
