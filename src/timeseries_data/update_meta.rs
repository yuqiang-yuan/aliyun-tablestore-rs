use prost::Message;

use crate::{add_per_request_options, error::OtsError, protos::timeseries::UpdateTimeseriesMetaResponse, timeseries_model::{rules::validate_timeseries_table_name, TimeseriesMeta, TimeseriesVersion}, OtsClient, OtsOp, OtsRequest, OtsResult};

/// 更新时间线元数据。如果更新的时间线元数据不存在，则直接执行新增操作。
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/updatetimeseriesmeta>
#[derive(Debug, Default, Clone)]
pub struct UpdateTimeseriesMetaRequest {
    /// 表名
    pub table_name: String,

    /// 要更新的时间线元数据列表。
    ///
    /// 注意，更新元数据的时候，**不可以**设置 meta 的 `update_time_us` 属性
    pub metas: Vec<TimeseriesMeta>,

    /// 时序表模型版本号。官方文档上有这个属性，但是 Java SDK 中是没有这个属性的。
    pub supported_table_version: TimeseriesVersion,
}

impl UpdateTimeseriesMetaRequest {
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

    /// 设置支持的版本号
    pub fn supported_table_version(mut self, ver: TimeseriesVersion) -> Self {
        self.supported_table_version = ver;

        self
    }

    /// 添加一个时间线元数据
    pub fn meta(mut self, meta: TimeseriesMeta) -> Self {
        self.metas.push(meta);

        self
    }

    /// 设置药更新的时间线元数据
    pub fn metas(mut self, metas: impl IntoIterator<Item = TimeseriesMeta>) -> Self {
        self.metas = metas.into_iter().collect();

        self
    }

    pub(crate) fn validate(&self) -> OtsResult<()> {
        if !validate_timeseries_table_name(&self.table_name) {
            return Err(OtsError::ValidationFailed(format!("invalid timeseries table name: {}", self.table_name)));
        }

        if self.metas.is_empty() {
            return Err(OtsError::ValidationFailed("metas can not be empty".to_string()));
        }

        for m in &self.metas {
            m.key.validate()?;

            if m.update_time_us.is_some() {
                return Err(OtsError::ValidationFailed("please do not set `update_time_us` when update meta".to_string()))
            }
        }

        Ok(())
    }
}


impl From<UpdateTimeseriesMetaRequest> for crate::protos::timeseries::UpdateTimeseriesMetaRequest {
    fn from(value: UpdateTimeseriesMetaRequest) -> Self {
        let UpdateTimeseriesMetaRequest {
            table_name,
            metas,
            supported_table_version,
        } = value;

        Self {
            table_name,
            timeseries_meta: todo!(),
            supported_table_version: todo!(),
        }
    }
}


#[derive(Debug, Default, Clone)]
pub struct UpdateTimeseriesMetaOperation {
    client: OtsClient,
    request: UpdateTimeseriesMetaRequest,
}

add_per_request_options!(UpdateTimeseriesMetaOperation);

impl UpdateTimeseriesMetaOperation {
    pub(crate) fn new(client: OtsClient, request: UpdateTimeseriesMetaRequest) -> Self {
        Self { client, request }
    }

    pub async fn send(self) -> OtsResult<UpdateTimeseriesMetaResponse> {
        self.request.validate()?;

        let Self { client, request } = self;

        let msg = crate::protos::timeseries::UpdateTimeseriesMetaRequest::from(request);

        let req = OtsRequest {
            operation: OtsOp::UpdateTimeseriesMeta,
            body: msg.encode_to_vec(),
            ..Default::default()
        };

        let resp = client.send(req).await?;

        Ok(crate::protos::timeseries::UpdateTimeseriesMetaResponse::decode(resp.bytes().await?)?)
    }
}
