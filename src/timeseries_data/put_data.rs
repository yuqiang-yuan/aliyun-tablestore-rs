use prost::Message;

use crate::{add_per_request_options, error::OtsError, model::Row, protos::{plain_buffer::{MASK_HEADER, MASK_ROW_CHECKSUM}, timeseries::MetaUpdateMode}, timeseries_model::{self, rules::validate_timeseries_table_name, TimeseriesRow, TimeseriesVersion}, util::debug_bytes, OtsClient, OtsOp, OtsRequest, OtsResult};

/// 写入时序数据。目前暂时只支持 plain buffer 编码
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/puttimeseriesdata>
#[derive(Debug, Default, Clone)]
pub struct PutTimeseriesDataRequest {
    /// 时序表名称
    pub table_name: String,

    /// 要插入的行数据
    pub rows: Vec<TimeseriesRow>,

    /// 元数据更新模式
    pub meta_update_mode: Option<MetaUpdateMode>,

    /// 时序表模型版本号
    pub supported_table_version: TimeseriesVersion,
}

impl PutTimeseriesDataRequest {
    pub fn new(table_name: &str) -> Self {
        Self {
            table_name: table_name.to_string(),
            ..Default::default()
        }
    }

    /// 设置表名
    pub fn table_name(mut self, table_name: &str) -> Self {
        self.table_name = table_name.to_string();

        self
    }

    /// 添加一行数据
    pub fn row(mut self, row: TimeseriesRow) -> Self {
        self.rows.push(row);

        self
    }

    /// 设置所有的行
    pub fn rows(mut self, rows: impl IntoIterator<Item = TimeseriesRow>) -> Self {
        self.rows = rows.into_iter().collect();

        self
    }

    /// 设置元数据更新模式
    pub fn meta_update_mode(mut self, meta_update_mode: MetaUpdateMode) -> Self {
        self.meta_update_mode = Some(meta_update_mode);

        self
    }

    /// 设置时序表模型版本
    pub fn supported_table_version(mut self, ver: TimeseriesVersion) -> Self {
        self.supported_table_version = ver;

        self
    }

    pub(crate) fn validate(&self) -> OtsResult<()> {
        if self.rows.is_empty() {
            return Err(OtsError::ValidationFailed("can not put empty rows to timeseries table".to_string()));
        }

        if !validate_timeseries_table_name(&self.table_name) {
            return Err(OtsError::ValidationFailed(format!("invalid timeseries table name: {}", self.table_name)));
        }

        if self.rows.len() > timeseries_model::rules::MAX_ROW_COUNT {
            return Err(OtsError::ValidationFailed(format!("rows count exceeds max rows count allowed: {}", timeseries_model::rules::MAX_ROW_COUNT)));
        }

        for row in &self.rows {
            row.validate()?;
        }

        Ok(())
    }
}

impl From<PutTimeseriesDataRequest> for crate::protos::timeseries::PutTimeseriesDataRequest {
    fn from(value: PutTimeseriesDataRequest) -> Self {
        let PutTimeseriesDataRequest {
            table_name,
            rows,
            meta_update_mode,
            supported_table_version,
        } = value;

        let ts_rows = rows.into_iter().map(crate::model::Row::from).collect::<Vec<_>>();
        log::debug!("{:#?}", ts_rows);
        let rows_data = Row::encode_plain_buffer_for_rows(ts_rows, MASK_HEADER | MASK_ROW_CHECKSUM);

        debug_bytes(rows_data.as_slice());

        Self {
            table_name,
            rows_data: crate::protos::timeseries::TimeseriesRows {
                r#type: crate::protos::timeseries::RowsSerializeType::RstPlainBuffer as i32,
                rows_data,
                // 虽然这个值是可选的，但是如果传入 None 会报错："Failed to parse the ProtoBuf message."
                flatbuffer_crc32c: Some(0),
            },
            meta_update_mode: meta_update_mode.map(|m| m as i32),
            supported_table_version: Some(supported_table_version as i64),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PutTimeseriesDataOperation {
    client: OtsClient,
    request: PutTimeseriesDataRequest,
}

add_per_request_options!(PutTimeseriesDataOperation);

impl PutTimeseriesDataOperation {
    pub(crate) fn new(client: OtsClient, request: PutTimeseriesDataRequest) -> Self {
        Self { client, request }
    }

    pub async fn send(self) -> OtsResult<crate::protos::timeseries::PutTimeseriesDataResponse> {
        self.request.validate()?;

        let Self { client, request } = self;

        let msg = crate::protos::timeseries::PutTimeseriesDataRequest::from(request);

        let req = OtsRequest {
            operation: OtsOp::PutTimeseriesData,
            body: msg.encode_to_vec(),
            ..Default::default()
        };

        let resp = client.send(req).await?;

        Ok(crate::protos::timeseries::PutTimeseriesDataResponse::decode(resp.bytes().await?)?)
    }
}
