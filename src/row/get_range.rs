use prost::Message;

use crate::{
    OtsClient, OtsOp, OtsRequest, OtsResult, add_per_request_options,
    error::OtsError,
    model::{PrimaryKey, PrimaryKeyColumn},
    protos::table_store::{Direction, GetRangeRequest, TimeRange},
};

/// 读取指定主键范围内的数据。
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/getrange>
#[derive(Default, Debug, Clone)]
pub struct GetRangeOperation {
    client: OtsClient,
    pub table_name: String,

    /// 本次查询的顺序。见 [`direction`](`Self::direction`)
    pub direction: Direction,

    /// 需要返回的全部列的列名。见 [`add_column_to_get`](`Self::add_column_to_get`)
    pub columns: Vec<String>,

    /// TimeRange 设置。和 `max_versions` 只能存在一个。
    ///
    /// 读取数据的版本时间戳范围。时间戳的单位为毫秒，取值最小值为0，最大值为INT64.MAX。
    pub time_range_start_ms: Option<i64>,
    pub time_range_end_ms: Option<i64>,
    pub time_range_specific_ms: Option<i64>,

    /// 本次范围读取的起始主键，包含
    pub inclusive_start_primary_key: Vec<PrimaryKeyColumn>,

    /// 本次范围读取的终止主键，不包含
    pub exclusive_end_primary_key: Vec<PrimaryKeyColumn>,

    /// 最多返回的版本个数。
    pub max_versions: Option<i32>,

    /// 本次读取最多返回的行数。取值必须大于 0。如果查询到的行数超过此值，则通过响应中会包含断点来记录本次读取到的位置，以便下一次读取。
    /// 无论是否设置此项，表格存储最多返回的行数为 5000 且总数据大小不超过 4 MB。
    pub limit: Option<i32>,

    /// 指定读取时的起始列，主要用于宽行读。返回结果中会包含当前起始列。列的顺序按照列名的字典序排序。
    pub start_column: Option<String>,

    /// 指定读取时的结束列，主要用于宽行读。返回结果中不会包含当前结束列。列的顺序按照列名的字典序排序。
    pub end_column: Option<String>,

    /// 启用本地事务时使用
    pub transaction_id: Option<String>,
}

add_per_request_options!(GetRangeOperation);

impl GetRangeOperation {
    pub(crate) fn new(client: OtsClient, table_name: &str) -> Self {
        Self {
            client,
            table_name: table_name.to_string(),
            ..Default::default()
        }
    }

    /// 本次查询的顺序。
    ///
    /// - 如果设置此项为 `FORWARD`（正序），则 `inclusive_start_primary` 必须小于 `exclusive_end_primary`，响应中各行按照主键由小到大的顺序进行排列。
    /// - 如果设置此项为 `BACKWARD`（逆序），则 `inclusive_start_primary` 必须大于 `exclusive_end_primary`，响应中各行按照主键由大到小的顺序进行排列。
    pub fn direction(mut self, direction: Direction) -> Self {
        self.direction = direction;

        self
    }

    /// 添加字符串类型的开始主键查询值。本次范围读取的起始主键，如果该行存在，则响应中一定会包含此行。
    pub fn add_string_start_pk_value(mut self, pk_name: &str, pk_value: impl Into<String>) -> Self {
        self.inclusive_start_primary_key.push(PrimaryKeyColumn::with_string_value(pk_name, pk_value));
        self
    }

    /// 添加整数类型的开始主键查询值。本次范围读取的起始主键，如果该行存在，则响应中一定会包含此行。
    pub fn add_integer_start_pk_value(mut self, pk_name: &str, pk_value: i64) -> Self {
        self.inclusive_start_primary_key.push(PrimaryKeyColumn::with_integer_value(pk_name, pk_value));

        self
    }

    /// 添加二进制类型的开始主键查询值。本次范围读取的起始主键，如果该行存在，则响应中一定会包含此行。
    pub fn add_binary_start_pk_value(mut self, pk_name: &str, pk_value: impl Into<Vec<u8>>) -> Self {
        self.inclusive_start_primary_key.push(PrimaryKeyColumn::with_binary_value(pk_name, pk_value));

        self
    }

    /// 添加无穷小值开始主键
    pub fn add_inf_min_start_pk_value(mut self, pk_name: &str) -> Self {
        self.inclusive_start_primary_key.push(PrimaryKeyColumn::with_infinite_min(pk_name));

        self
    }

    /// 添加无穷大值开始主键
    pub fn add_inf_max_start_pk_value(mut self, pk_name: &str) -> Self {
        self.inclusive_start_primary_key.push(PrimaryKeyColumn::with_infinite_max(pk_name));

        self
    }

    /// 添加字符串类型的结束主键查询值。无论该行是否存在，则响应中一定不会包含此行。
    pub fn add_string_end_pk_value(mut self, pk_name: &str, pk_value: impl Into<String>) -> Self {
        self.exclusive_end_primary_key.push(PrimaryKeyColumn::with_string_value(pk_name, pk_value));
        self
    }

    /// 添加整数类型的结束主键查询值。无论该行是否存在，则响应中一定不会包含此行。
    pub fn add_integer_end_pk_value(mut self, pk_name: &str, pk_value: i64) -> Self {
        self.exclusive_end_primary_key.push(PrimaryKeyColumn::with_integer_value(pk_name, pk_value));

        self
    }

    /// 添加二进制类型的结束主键查询值。无论该行是否存在，则响应中一定不会包含此行。
    pub fn add_binary_end_pk_value(mut self, pk_name: &str, pk_value: impl Into<Vec<u8>>) -> Self {
        self.exclusive_end_primary_key.push(PrimaryKeyColumn::with_binary_value(pk_name, pk_value));

        self
    }

    /// 添加无穷小值结束主键
    pub fn add_inf_min_end_pk_value(mut self, pk_name: &str) -> Self {
        self.exclusive_end_primary_key.push(PrimaryKeyColumn::with_infinite_min(pk_name));

        self
    }

    /// 添加无穷大值结束主键
    pub fn add_inf_max_end_pk_value(mut self, pk_name: &str) -> Self {
        self.exclusive_end_primary_key.push(PrimaryKeyColumn::with_infinite_max(pk_name));

        self
    }

    /// 需要返回的全部列的列名。如果为空，则返回指定行的所有列。`columns_to_get` 个数不应超过128个。
    /// 如果指定的列不存在，则不会返回指定列的数据；如果给出了重复的列名，返回结果只会包含一次指定列。
    pub fn add_column_to_get(mut self, col_name: &str) -> Self {
        self.columns.push(col_name.to_string());

        self
    }

    /// 读取数据时，返回的最多版本个数。和 `time_range` 至少存在一个
    /// 如果指定 `max_versions` 为 `2` ，则每一列最多返回 `2` 个版本的数据。
    pub fn max_versions(mut self, max_versions: i32) -> Self {
        self.max_versions = Some(max_versions);

        self
    }

    /// 本次读取最多返回的行数。取值必须大于0。
    /// 如果查询到的行数超过此值，则通过响应中会包含断点来记录本次读取到的位置，以便下一次读取。
    /// 无论是否设置此项，表格存储最多返回的行数为 5000 且总数据大小不超过 4 MB。
    pub fn limit(mut self, limit: i32) -> Self {
        self.limit = Some(limit);

        self
    }

    /// 查询数据时指定的时间戳范围 `[start_time, end_time)` 或特定时间戳值 `time_specific`。
    /// 时间范围和特定时间戳值二者指定其一即可。
    ///
    /// - `start_ms`: 起始时间戳。单位是毫秒。时间戳的取值最小值为 `0`，最大值为 [`i64::MAX`](`std::i64::MAX`)。
    /// - `end_ms`: 结束时间戳。单位是毫秒。时间戳的取值最小值为 `0`，最大值为 [`i64::MAX`](`std::i64::MAX`)。
    pub fn time_range(mut self, start_ms: i64, end_ms: i64) -> Self {
        self.time_range_start_ms = Some(start_ms);
        self.time_range_end_ms = Some(end_ms);

        self
    }

    /// 指定精确的时间戳
    pub fn specific_time_ms(mut self, time_ms: i64) -> Self {
        self.time_range_specific_ms = Some(time_ms);

        self
    }

    /// 指定读取时的起始列，主要用于宽行读。列的顺序按照列名的字典序排序。返回的结果中**包含**当前起始列。
    /// 如果一张表有 `a` 、 `b` 、 `c` 三列，读取时指定 `start_column` 为 `b` ，则会从 `b` 列开始读，返回 `b`、`c` 两列。
    pub fn start_column(mut self, col_name: impl Into<String>) -> Self {
        self.start_column = Some(col_name.into());

        self
    }

    /// 返回的结果中**不包含**当前结束列。列的顺序按照列名的字典序排序。
    /// 如果一张表有 `a` 、 `b` 、 `c` 三列，读取时指定 `end_column` 为 `b`，则读到 `b` 列时会结束，返回 `a` 列。
    pub fn end_column(mut self, col_name: impl Into<String>) -> Self {
        self.end_column = Some(col_name.into());

        self
    }

    /// 局部事务ID。当使用局部事务功能读取数据时必须设置此参数。
    pub fn transaction_id(mut self, tx_id: impl Into<String>) -> Self {
        self.transaction_id = Some(tx_id.into());

        self
    }

    pub async fn send(self) -> OtsResult<crate::model::GetRangeResponse> {
        let Self {
            client,
            inclusive_start_primary_key,
            exclusive_end_primary_key,
            max_versions,
            direction,
            columns,
            time_range_start_ms,
            time_range_end_ms,
            time_range_specific_ms,
            limit,
            start_column,
            end_column,
            table_name,
            transaction_id,
        } = self;

        if max_versions.is_some() && (time_range_start_ms.is_some() || time_range_end_ms.is_some() || time_range_specific_ms.is_some()) {
            return Err(OtsError::ValidationFailed(
                "`max_versions` and `time_range` can not exist at the same time".to_string(),
            ));
        }

        let start_pk = PrimaryKey {
            keys: inclusive_start_primary_key,
        };

        let end_pk = PrimaryKey {
            keys: exclusive_end_primary_key,
        };

        let start_pk_bytes = start_pk.into_plain_buffer(true);
        let end_pk_bytes = end_pk.into_plain_buffer(true);

        let msg = GetRangeRequest {
            table_name,
            direction: direction as i32,
            columns_to_get: columns,
            time_range: if time_range_start_ms.is_some() || time_range_end_ms.is_some() || time_range_specific_ms.is_some() {
                Some(TimeRange {
                    start_time: time_range_start_ms,
                    end_time: time_range_end_ms,
                    specific_time: time_range_specific_ms,
                })
            } else {
                None
            },
            max_versions,
            limit,
            inclusive_start_primary_key: start_pk_bytes,
            exclusive_end_primary_key: end_pk_bytes,
            filter: None,
            start_column,
            end_column,
            token: None,
            transaction_id,
        };

        let req = OtsRequest {
            operation: OtsOp::GetRange,
            body: msg.encode_to_vec(),
            ..Default::default()
        };

        let response = client.send(req).await?;

        crate::model::GetRangeResponse::decode(response.bytes().await?.to_vec())
    }
}
