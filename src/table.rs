//! Operations on tables. e.g. list table, create table and so on. 仅针对宽表模型

use std::collections::HashSet;

use prost::Message;
use reqwest::Method;
use rules::{MAX_PRIMARY_KEY_COUNT, MIN_PRIMARY_KEY_COUNT, validate_column_name, validate_index_name, validate_table_name};

use crate::{
    OtsClient, OtsOp, OtsRequest, OtsResult,
    error::OtsError,
    protos::table_store::{
        CapacityUnit, CreateTableRequest, CreateTableResponse, DefinedColumnSchema, DefinedColumnType, DescribeTableRequest, DescribeTableResponse, IndexMeta,
        ListTableRequest, ListTableResponse, PrimaryKeySchema, PrimaryKeyType, ReservedThroughput, SseKeyType, SseSpecification, StreamSpecification,
        TableMeta, TableOptions,
    },
};

/// Validation rules for table
pub(crate) mod rules {
    /// 一个宽表至少有 1 个主键列
    pub const MIN_PRIMARY_KEY_COUNT: usize = 1;

    /// 一个宽表最多 4 个主键列
    pub const MAX_PRIMARY_KEY_COUNT: usize = 4;

    /// 一个宽表二级索引个数最大值
    pub const MAX_INDEX_COUNT: usize = 4;

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

/// Create table
///
/// 根据官方文档 [https://help.aliyun.com/zh/tablestore/table-operations] 2025-03-06 10:05:03 更新的内容，在创建表的时候，支持设置以下内容：
///
/// - 主键
/// - 数据版本和生命周期
/// - 预留读写吞吐量
/// - 二级索引
/// - 数据加密
/// - 本地事务
///
/// 所以，虽然 `table_store.proto` 文件中的 `CreateTableRequest` 包含了分区相关的，但是这里没有放上来。对应的 Java SDK 5.17.5 版本中创建宽表的时候也是没有分区设定的。
#[derive(Default)]
pub struct CreateTableOperation {
    client: OtsClient,
    // table meta
    table_name: String,
    primary_keys: Vec<PrimaryKeySchema>,
    defined_columns: Vec<DefinedColumnSchema>,

    // reserved throughput
    reserved_throughput_read: Option<i32>,
    reserved_throughput_write: Option<i32>,

    // table options
    ttl_seconds: Option<i32>,
    max_versions: Option<i32>,
    deviation_cell_version_in_sec: Option<i64>,
    allow_update: Option<bool>,

    // stream spec.
    stream_enabled: bool,
    stream_expiration_hour: Option<i32>,
    stream_columns: HashSet<String>,

    // sse
    sse_enabled: bool,
    sse_key_type: Option<SseKeyType>,

    // required when sse_key_type is byok
    sse_key_id: Option<String>,

    // required when sse_key_type is byok
    sse_arn: Option<String>,

    // local tx
    enable_local_txn: Option<bool>,

    // indexes
    indexes: Vec<IndexMeta>,
}

impl CreateTableOperation {
    pub(crate) fn new(client: OtsClient, table_name: &str) -> Self {
        Self {
            client,
            table_name: table_name.to_string(),
            ttl_seconds: Some(-1),
            max_versions: Some(1),
            reserved_throughput_read: Some(0),
            reserved_throughput_write: Some(0),
            deviation_cell_version_in_sec: Some(86400),
            enable_local_txn: Some(false),
            ..Default::default()
        }
    }

    /// 添加主键列。一个表格至少包含 1 个主键列，最多包含 4 个主键列
    pub fn add_primary_key(mut self, name: impl Into<String>, key_type: PrimaryKeyType, auto_inc: bool) -> Self {
        let pk = PrimaryKeySchema {
            name: name.into(),
            r#type: key_type as i32,
            option: if auto_inc { Some(1i32) } else { None },
        };

        self.primary_keys.push(pk);
        self
    }

    /// Add defined column
    pub fn add_defined_column(mut self, name: impl Into<String>, col_type: DefinedColumnType) -> Self {
        let col = DefinedColumnSchema {
            name: name.into(),
            r#type: col_type as i32,
        };

        self.defined_columns.push(col);

        self
    }

    /// Set reserved throughput for reading. maximum: 100000 CU
    pub fn reserved_throughput_read(mut self, read_cu: i32) -> Self {
        self.reserved_throughput_read = Some(read_cu);
        self
    }

    /// Set reserved throughput for writing. maximum: 100000 CU
    pub fn reserved_throughput_write(mut self, write_cu: i32) -> Self {
        self.reserved_throughput_write = Some(write_cu);
        self
    }

    /// 数据生命周期，即数据的过期时间。当数据的保存时间超过设置的数据生命周期时，系统会自动清理超过数据生命周期的数据。
    /// 数据生命周期至少为 `86400` 秒（一天）或 `-1`（数据永不过期）。
    pub fn ttl_seconds(mut self, ttl_seconds: i32) -> Self {
        self.ttl_seconds = Some(ttl_seconds);
        self
    }

    /// 最大版本数，即属性列能够保留数据的最大版本个数。当属性列数据的版本个数超过设置的最大版本数时，系统会自动删除较早版本的数据。
    pub fn max_versions(mut self, max_versions: i32) -> Self {
        self.max_versions = Some(max_versions);
        self
    }

    /// 有效版本偏差，即写入数据的时间戳与系统当前时间的偏差允许最大值。只有当写入数据所有列的版本号与写入时时间的差值在数据有效版本偏差范围内，数据才能成功写入。
    ///
    /// 属性列的有效版本范围为 `[max{数据写入时间-有效版本偏差,数据写入时间-数据生命周期}，数据写入时间+有效版本偏差)`。
    pub fn deviation_cell_version_seconds(mut self, dev: i64) -> Self {
        self.deviation_cell_version_in_sec = Some(dev);
        self
    }

    /// 是否允许通过 `UpdateRow` 更新写入数据。默认值为 `true`，表示允许通过 `UpdateRow` 更新写入数据。
    ///
    /// 当要使用多元索引生命周期功能时，您必须设置此参数为 `false`，即不允许通过 `UpdateRow` 更新写入数据。
    pub fn allow_update(mut self, allow_update: bool) -> Self {
        self.allow_update = Some(allow_update);
        self
    }

    pub fn stream(mut self, enabled: bool) -> Self {
        self.stream_enabled = enabled;
        self
    }

    pub fn stream_expiration(mut self, exp: i32) -> Self {
        self.stream_expiration_hour = Some(exp);
        self
    }

    pub fn add_stream_column(mut self, col_name: impl Into<String>) -> Self {
        self.stream_columns.insert(col_name.into());
        self
    }

    pub fn sse(mut self, enabled: bool) -> Self {
        self.sse_enabled = enabled;
        self
    }

    pub fn sse_key_type(mut self, key_type: SseKeyType) -> Self {
        self.sse_key_type = Some(key_type);
        self
    }

    pub fn sse_key_id(mut self, key_id: impl Into<String>) -> Self {
        self.sse_key_id = Some(key_id.into());
        self
    }

    pub fn sse_arn(mut self, arn: impl Into<String>) -> Self {
        self.sse_arn = Some(arn.into());
        self
    }

    /// Enable or disable local transaction
    pub fn local_txn(mut self, enabled: bool) -> Self {
        self.enable_local_txn = Some(enabled);
        self
    }

    /// Add a new index meta. You can use [`IndexMetaBuilder`] for chaining call.
    pub fn add_index(mut self, idx_meta: IndexMeta) -> Self {
        self.indexes.push(idx_meta);
        self
    }

    /// Validate the create table settings
    fn validate(&self) -> OtsResult<()> {
        if !validate_table_name(&self.table_name) {
            return Err(OtsError::ValidationFailed(format!("invalid table name: \"{}\"", self.table_name)));
        }

        if !(MIN_PRIMARY_KEY_COUNT..=MAX_PRIMARY_KEY_COUNT).contains(&self.primary_keys.len()) {
            return Err(OtsError::ValidationFailed(format!("invalid primary key count: {}", self.primary_keys.len())));
        }

        if let Some(n) = &self.ttl_seconds {
            if *n != -1 && *n < 86400 {
                return Err(OtsError::ValidationFailed(format!("invalid time-to-live settings: {}", *n)));
            }
        }

        for col in &self.defined_columns {
            if !validate_column_name(&col.name) {
                return Err(OtsError::ValidationFailed(format!("invalid column name: \"{}\"", col.name)));
            }
        }

        if self.sse_enabled {
            if let Some(SseKeyType::SseByok) = self.sse_key_type {
                if self.sse_key_id.is_none() || self.sse_arn.is_none() {
                    return Err(OtsError::ValidationFailed(
                        "You have SSE Enabled and key type is BYOK, but sse key id and ARN are not set".to_string(),
                    ));
                }
            }
        }

        let pk_names = self.primary_keys.iter().map(|k| k.name.as_str()).collect::<Vec<_>>();

        let col_names = self.defined_columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>();

        if !self.indexes.iter().all(|idx| {
            idx.primary_key.iter().all(|k| pk_names.contains(&k.as_str()))
                && idx.defined_column.iter().all(|c| col_names.contains(&c.as_str()))
                && validate_index_name(&idx.name)
        }) {
            return Err(OtsError::ValidationFailed(
                "invalid index meta. Can not find primary key or defined column with the name speicfied on index meta, or, the index name is invalid"
                    .to_string(),
            ));
        }

        Ok(())
    }

    pub async fn send(self) -> OtsResult<CreateTableResponse> {
        self.validate()?;

        let Self {
            client,
            table_name,
            primary_keys,
            defined_columns,
            reserved_throughput_read,
            reserved_throughput_write,
            ttl_seconds,
            max_versions,
            deviation_cell_version_in_sec,
            allow_update,
            stream_enabled,
            stream_expiration_hour,
            stream_columns,
            sse_enabled,
            sse_key_type,
            sse_key_id,
            sse_arn,
            enable_local_txn,
            indexes,
        } = self;

        let msg = CreateTableRequest {
            table_meta: TableMeta {
                table_name,
                primary_key: primary_keys,
                defined_column: defined_columns,
            },
            reserved_throughput: ReservedThroughput {
                capacity_unit: CapacityUnit {
                    read: reserved_throughput_read,
                    write: reserved_throughput_write,
                },
            },
            table_options: if ttl_seconds.is_some() || max_versions.is_some() || deviation_cell_version_in_sec.is_some() || allow_update.is_some() {
                Some(TableOptions {
                    time_to_live: ttl_seconds,
                    max_versions,
                    deviation_cell_version_in_sec,
                    allow_update,
                    update_full_row: None,
                })
            } else {
                None
            },
            partitions: vec![],
            stream_spec: if stream_enabled {
                Some(StreamSpecification {
                    enable_stream: stream_enabled,
                    expiration_time: stream_expiration_hour,
                    columns_to_get: stream_columns.into_iter().collect::<Vec<_>>(),
                })
            } else {
                None
            },
            sse_spec: if sse_enabled {
                Some(SseSpecification {
                    enable: sse_enabled,
                    key_type: sse_key_type.map(|v| v as i32),
                    key_id: sse_key_id.map(|v| v.into_bytes()),
                    role_arn: sse_arn.map(|v| v.into_bytes()),
                })
            } else {
                None
            },
            index_metas: indexes,
            enable_local_txn,
        };

        // let bytes = msg.encode_to_vec();

        // std::fs::write("/home/yuanyq/Downloads/protobuf-test/create-table.data", bytes).unwrap();

        // Err(OtsError::ValidationFailed("".to_string()))
        log::debug!("create table message: {:#?}", msg);

        let req = OtsRequest {
            method: Method::POST,
            operation: OtsOp::CreateTable,
            body: msg.encode_to_vec().into(),
            ..Default::default()
        };

        let response = client.send(req).await?;

        Ok(CreateTableResponse::decode(response.bytes().await?)?)
    }
}

/// List table
pub struct ListTableOperation {
    client: OtsClient,
}

impl ListTableOperation {
    pub(crate) fn new(client: OtsClient) -> Self {
        Self { client }
    }

    /// Consume the builder and send request
    pub async fn send(self) -> OtsResult<ListTableResponse> {
        let msg = ListTableRequest {};
        let req = OtsRequest {
            method: Method::POST,
            operation: OtsOp::ListTable,
            body: msg.encode_to_vec().into(),
            ..Default::default()
        };

        let Self { client } = self;

        let response = client.send(req).await?;
        Ok(ListTableResponse::decode(response.bytes().await?)?)
    }
}

/// Describe table
pub struct DescribeTableOperation {
    client: OtsClient,
    table_name: String,
}

impl DescribeTableOperation {
    pub(crate) fn new(client: OtsClient, table_name: &str) -> Self {
        Self {
            client,
            table_name: table_name.to_string(),
        }
    }

    pub async fn send(self) -> OtsResult<DescribeTableResponse> {
        let Self { client, table_name } = self;

        let body = DescribeTableRequest { table_name }.encode_to_vec();

        let req = OtsRequest {
            method: Method::POST,
            operation: OtsOp::DescribeTable,
            body: body.into(),
            ..Default::default()
        };

        let response = client.send(req).await?;

        Ok(DescribeTableResponse::decode(response.bytes().await?)?)
    }
}

impl OtsClient {
    /// Create table
    pub fn create_table(&self, table_name: &str) -> CreateTableOperation {
        CreateTableOperation::new(self.clone(), table_name)
    }

    /// List tables in a instance
    pub fn list_table(&self) -> ListTableOperation {
        ListTableOperation::new(self.clone())
    }

    /// Describe table
    pub fn describe_table(&self, table_name: &str) -> DescribeTableOperation {
        DescribeTableOperation::new(self.clone(), table_name)
    }
}

#[cfg(test)]
mod test_table {
    use std::sync::Once;

    use crate::{
        OtsClient,
        protos::table_store::{DefinedColumnType, PrimaryKeyType},
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

    #[tokio::test]
    async fn test_create_table() {
        setup();
        let client = OtsClient::from_env();

        let response = client
            .create_table("ccs1")
            .add_primary_key("cc_id", PrimaryKeyType::String, false)
            .add_primary_key("school_id", PrimaryKeyType::String, false)
            .add_primary_key("creator_id", PrimaryKeyType::String, false)
            .add_defined_column("invitation_code", DefinedColumnType::DctString)
            .add_defined_column("course_name", DefinedColumnType::DctString)
            .add_defined_column("status", DefinedColumnType::DctString)
            .send()
            .await;

        log::debug!("{:#?}", response);

        assert!(response.is_ok());
    }
}
