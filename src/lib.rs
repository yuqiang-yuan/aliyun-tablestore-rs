use std::{collections::HashMap, fmt::Display, str::FromStr, time::Duration};

use base64::{Engine, prelude::BASE64_STANDARD};
use bytes::Bytes;
use defined_column::{AddDefinedColumnOperation, DeleteDefinedColumnOperation};
use error::OtsError;
use prost::Message;
use protos::table_store::{self};
use reqwest::{
    Response,
    header::{HeaderMap, HeaderName, HeaderValue},
};

use data::{GetRangeOperation, GetRowOperation, PutRowOperation, UpdateRowOperation};
use table::{ComputeSplitPointsBySizeOperation, CreateTableOperation, DeleteTableOperation, DescribeTableOperation, ListTableOperation, UpdateTableOperation};
use url::Url;
use util::get_iso8601_date_time_string;

pub mod crc8;
pub mod data;
pub mod defined_column;
pub mod error;
pub mod index;
pub mod macros;
pub mod model;
pub mod protos;
pub mod table;
pub mod util;

const USER_AGENT: &str = "aliyun-tablestore-rs/0.1.0";
const HEADER_API_VERSION: &str = "x-ots-apiversion";
const HEADER_ACCESS_KEY_ID: &str = "x-ots-accesskeyid";
const HEADER_CONTENT_MD5: &str = "x-ots-contentmd5";
const HEADER_SIGNATURE: &str = "x-ots-signature";
const HEADER_DATE: &str = "x-ots-date";
const HEADER_STS_TOKEN: &str = "x-ots-ststoken";
const HEADER_INSTANCE_NAME: &str = "x-ots-instancename";

const API_VERSION: &str = "2015-12-31";

pub type OtsResult<T> = Result<T, OtsError>;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtsOp {
    #[default]
    Undefined,

    // tables
    CreateTable,
    UpdateTable,
    ListTable,
    DescribeTable,
    DeleteTable,
    ComputeSplitPointsBySize,

    // defined columns
    AddDefinedColumn,
    DeleteDefinedColumn,

    // Data operations
    GetRow,
    GetRange,
    PutRow,
    UpdateRow,
    DeleteRow,
    BatchGetRow,
    BatchWriteRow,
}

impl From<OtsOp> for String {
    fn from(value: OtsOp) -> Self {
        value.to_string()
    }
}

impl Display for OtsOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            OtsOp::Undefined => "_Undefined_",

            OtsOp::CreateTable => "CreateTable",
            OtsOp::UpdateTable => "UpdateTable",
            OtsOp::ListTable => "ListTable",
            OtsOp::DescribeTable => "DescribeTable",
            OtsOp::DeleteTable => "DeleteTable",
            OtsOp::ComputeSplitPointsBySize => "ComputeSplitPointsBySize",

            OtsOp::AddDefinedColumn => "AddDefinedColumn",
            OtsOp::DeleteDefinedColumn => "DeleteDefinedColumn",

            OtsOp::GetRow => "GetRow",
            OtsOp::GetRange => "GetRange",
            OtsOp::PutRow => "PutRow",
            OtsOp::UpdateRow => "UpdateRow",
            OtsOp::DeleteRow => "DeleteRow",
            OtsOp::BatchGetRow => "BatchGetRow",
            OtsOp::BatchWriteRow => "BatchWriteRow",
        };

        write!(f, "{}", s)
    }
}

/// The request to send to aliyun tablestore service
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct OtsRequest {
    method: reqwest::Method,
    operation: OtsOp,
    headers: HashMap<String, String>,
    query: HashMap<String, String>,
    body: Vec<u8>,
}

impl Default for OtsRequest {
    fn default() -> Self {
        Self {
            method: reqwest::Method::POST,
            operation: OtsOp::Undefined,
            headers: HashMap::new(),
            query: HashMap::new(),
            body: Vec::new(),
        }
    }
}

pub trait RetryPolicy: std::fmt::Debug + Send + Sync {
    fn should_retry(&self, op: OtsOp, api_error: crate::table_store::Error) -> bool;
    fn clone_box(&self) -> Box<dyn RetryPolicy>;
}

impl Clone for Box<dyn RetryPolicy> {
    fn clone(&self) -> Box<dyn RetryPolicy> {
        self.clone_box()
    }
}

#[derive(Debug)]
pub struct DefaultRetryPolicy;

impl RetryPolicy for DefaultRetryPolicy {
    fn should_retry(&self, _op: OtsOp, _api_error: crate::table_store::Error) -> bool {
        false
    }

    fn clone_box(&self) -> Box<dyn RetryPolicy> {
        Box::new(DefaultRetryPolicy)
    }
}

#[derive(Debug, Clone)]
pub struct OtsClientOptions {
    pub timeout_ms: Option<u64>,
    pub retry_policy: Box<dyn RetryPolicy>,
}

impl OtsClientOptions {
    pub fn new() -> Self {
        Self {
            retry_policy: Box::new(DefaultRetryPolicy),
            timeout_ms: None,
        }
    }
}

impl Default for OtsClientOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Aliyun tablestore client
#[allow(dead_code)]
#[derive(Clone, Default)]
pub struct OtsClient {
    access_key_id: String,
    access_key_secret: String,
    sts_token: Option<String>,
    region: String,
    instance_name: String,
    endpoint: String,
    http_client: reqwest::Client,
    options: OtsClientOptions,
}

impl std::fmt::Debug for OtsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtsClient")
            .field("access_key_id", &self.access_key_id)
            .field("region", &self.region)
            .field("instance_name", &self.instance_name)
            .field("endpoint", &self.endpoint)
            .field("http_client", &self.http_client)
            .field("options", &self.options)
            .finish()
    }
}

impl OtsClient {
    fn parse_instance_and_region(endpoint: &str) -> (&str, &str) {
        let s = endpoint.strip_prefix("http://").unwrap_or(endpoint);
        let s = s.strip_prefix("https://").unwrap_or(s);
        let parts = s.split(".").collect::<Vec<_>>();
        if parts.len() < 2 {
            panic!("can not parse instance name and region from endpoint: {}", endpoint);
        }

        (parts[0], parts[1])
    }

    /// Build an OtsClient from env values. The following env vars are required:
    ///
    /// - `ALIYUN_OTS_AK_ID`: The access key id.
    /// - `ALIYUN_OTS_AK_SEC`: The access key secret
    /// - `ALIYUN_OTS_ENDPOINT`: The tablestore instance endpoint. e.g. `https://${instance-name}.cn-beijing.ots.aliyuncs.com`
    pub fn from_env() -> Self {
        let access_key_id = std::env::var("ALIYUN_OTS_AK_ID").expect("env var ALI_ACCESS_KEY_ID is missing");
        let access_key_secret = std::env::var("ALIYUN_OTS_AK_SEC").expect("env var ALI_ACCESS_KEY_SECRET is missing");
        let endpoint = std::env::var("ALIYUN_OTS_ENDPOINT").expect("env var ALI_OSS_ENDPOINT is missing");
        let endpoint = endpoint.to_lowercase();
        let (instance_name, region) = Self::parse_instance_and_region(endpoint.as_str());

        Self {
            access_key_id,
            access_key_secret,
            sts_token: None,
            region: region.to_string(),
            instance_name: instance_name.to_string(),
            endpoint,
            http_client: reqwest::Client::new(),
            options: OtsClientOptions::default(),
        }
    }

    fn prepare_headers(&self, req: &mut OtsRequest) {
        let headers = &mut req.headers;
        headers.insert("User-Agent".to_string(), USER_AGENT.to_string());
        headers.insert(HEADER_API_VERSION.to_string(), API_VERSION.to_string());
        headers.insert(HEADER_DATE.to_string(), get_iso8601_date_time_string());
        headers.insert(HEADER_ACCESS_KEY_ID.to_string(), self.access_key_id.clone());
        headers.insert(HEADER_INSTANCE_NAME.to_string(), self.instance_name.clone());

        if let Some(s) = &self.sts_token {
            headers.insert(HEADER_STS_TOKEN.to_string(), s.to_string());
        }

        let body_bytes = &req.body;

        log::debug!("body bytes: {:?}", body_bytes);

        headers.insert("Content-Length".to_string(), format!("{}", body_bytes.len()));
        let content_md5_base64 = BASE64_STANDARD.encode(md5::compute(body_bytes).as_slice());
        headers.insert(HEADER_CONTENT_MD5.to_string(), content_md5_base64);
    }

    fn header_sign(&self, req: &mut OtsRequest) {
        self.prepare_headers(req);

        let mut canonical_headers = req
            .headers
            .iter()
            .map(|(k, v)| (k.to_lowercase(), v))
            .filter(|(k, _)| k.starts_with("x-ots-") && k != HEADER_SIGNATURE)
            .map(|(k, v)| format!("{}:{}", k, v))
            .collect::<Vec<_>>();
        canonical_headers.sort();

        let canonical_headers = canonical_headers.join("\n");

        let string_to_sign = format!("/{}\n{}\n\n{}\n", req.operation, req.method, canonical_headers);

        log::debug!("string to sign: ({})", string_to_sign);
        let sig = util::hmac_sha1(self.access_key_secret.as_bytes(), string_to_sign.as_bytes());
        let sig_string = BASE64_STANDARD.encode(&sig);

        log::debug!("signature = {}", sig_string);

        req.headers.insert(HEADER_SIGNATURE.to_string(), sig_string);
    }

    pub async fn send(&self, req: OtsRequest) -> OtsResult<Response> {
        let mut req = req;
        self.header_sign(&mut req);

        let OtsRequest {
            method,
            operation,
            headers,
            query: _,
            body,
        } = req;

        let mut header_map = HeaderMap::new();
        headers.into_iter().for_each(|(k, v)| {
            log::debug!(">> header: {}: {}", k, v);
            header_map.insert(HeaderName::from_str(&k.to_lowercase()).unwrap(), HeaderValue::from_str(&v).unwrap());
        });

        let request_body = Bytes::from_owner(body);
        let url = Url::parse(format!("{}/{}", self.endpoint, operation).as_str()).unwrap();

        let mut request_builder = self
            .http_client
            .request(method, url.clone())
            .headers(header_map.clone())
            .body(request_body.clone());

        // Handle per-request options
        if let Some(ms) = self.options.timeout_ms {
            request_builder = request_builder.timeout(Duration::from_millis(ms));
        }

        let response = request_builder.send().await?;

        response.headers().iter().for_each(|(k, v)| {
            log::debug!("<< header: {}: {}", k, v.to_str().unwrap());
        });

        if !&response.status().is_success() {
            let status = response.status();

            match response.bytes().await {
                Ok(bytes) => {
                    let api_error = table_store::Error::decode(bytes)?;
                    return Err(OtsError::ApiError(Box::new(api_error)));
                }
                Err(_) => return Err(OtsError::StatusError(status, "".to_string())),
            }
        }

        Ok(response)
    }

    /// 列出实例下的宽表
    pub fn list_table(&self) -> ListTableOperation {
        ListTableOperation::new(self.clone())
    }

    /// 创建一个宽表
    pub fn create_table(&self, table_name: &str) -> CreateTableOperation {
        CreateTableOperation::new(self.clone(), table_name)
    }

    /// 更新宽表定义
    pub fn update_table(&self, table_name: &str) -> UpdateTableOperation {
        UpdateTableOperation::new(self.clone(), table_name)
    }

    /// 获取宽表定义
    pub fn describe_table(&self, table_name: &str) -> DescribeTableOperation {
        DescribeTableOperation::new(self.clone(), table_name)
    }

    /// 删除宽表
    pub fn delete_table(&self, table_name: &str) -> DeleteTableOperation {
        DeleteTableOperation::new(self.clone(), table_name)
    }

    /// 计算宽表分裂点
    pub fn compute_split_points_by_size(&self, table_name: &str, size: u64) -> ComputeSplitPointsBySizeOperation {
        ComputeSplitPointsBySizeOperation::new(self.clone(), table_name, size)
    }

    /// 添加预定义列
    pub fn add_defined_column(&self, table_name: &str) -> AddDefinedColumnOperation {
        AddDefinedColumnOperation::new(self.clone(), table_name)
    }

    /// 删除预定义列
    pub fn delete_defined_column(&self, table_name: &str) -> DeleteDefinedColumnOperation {
        DeleteDefinedColumnOperation::new(self.clone(), table_name)
    }

    /// 根据主键获取单行数据
    pub fn get_row(&self, table_name: &str) -> GetRowOperation {
        GetRowOperation::new(self.clone(), table_name)
    }

    /// 根据主键获取范围数据
    pub fn get_range(&self, table_name: &str) -> GetRangeOperation {
        GetRangeOperation::new(self.clone(), table_name)
    }

    /// 插入一行数据
    pub fn put_row(&self, table_name: &str) -> PutRowOperation {
        PutRowOperation::new(self.clone(), table_name)
    }

    /// 更新一行数据
    pub fn update_row(&self, table_name: &str) -> UpdateRowOperation {
        UpdateRowOperation::new(self.clone(), table_name)
    }
}
