use std::collections::HashSet;

use prost::Message;

use crate::{
    OtsClient, OtsOp, OtsRequest, OtsResult, add_per_request_options,
    error::OtsError,
    model::PrimaryKey,
    protos::search::ColumnReturnType,
    table::rules::{validate_index_name, validate_table_name},
};

use super::SearchQuery;

/// 通过多元索引查询数据。
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/search>
#[derive(Debug, Clone)]
pub struct SearchRequest {
    /// 数据表名称。
    pub table_name: String,

    /// 多元索引名称。
    pub index_name: String,

    /// 查询配置
    pub search_query: SearchQuery,

    /// 路由键的值。默认为空，表示不使用路由键。大部分时候不需要使用此值
    pub routing_values: Vec<PrimaryKey>,

    /// 需要返回的全部列的列名
    pub columns_to_get: HashSet<String>,

    /// 列返回类型
    pub column_return_type: Option<ColumnReturnType>,

    /// 查询的超时时间。单位为毫秒。
    pub timeout_ms: Option<u32>,
}

impl SearchRequest {
    pub fn new(table_name: &str, index_name: &str, query: SearchQuery) -> Self {
        Self {
            table_name: table_name.to_string(),
            index_name: index_name.to_string(),
            search_query: query,
            routing_values: Vec::new(),
            columns_to_get: HashSet::new(),
            column_return_type: None,
            timeout_ms: None,
        }
    }

    /// 设置表名
    pub fn table_name(mut self, table_name: &str) -> Self {
        self.table_name = table_name.to_string();

        self
    }

    /// 设置索引名
    pub fn index_name(mut self, index_name: &str) -> Self {
        self.index_name = index_name.to_string();

        self
    }

    /// 设置查询配置
    pub fn search_query(mut self, query: SearchQuery) -> Self {
        self.search_query = query;

        self
    }

    /// 添加一个路由主键
    pub fn routing_value(mut self, pk: PrimaryKey) -> Self {
        self.routing_values.push(pk);

        self
    }

    /// 设置路由主键
    pub fn routing_values(mut self, pks: impl IntoIterator<Item = PrimaryKey>) -> Self {
        self.routing_values = pks.into_iter().collect();

        self
    }

    /// 设置列返回类型
    pub fn column_return_type(mut self, column_return_type: ColumnReturnType) -> Self {
        self.column_return_type = Some(column_return_type);

        self
    }

    /// 添加要返回的列名
    pub fn column_to_get(mut self, col: impl Into<String>) -> Self {
        self.columns_to_get.insert(col.into());

        self
    }

    /// 设置要返回的列名
    pub fn columns_to_get(mut self, cols: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns_to_get = cols.into_iter().map(|col| col.into()).collect();

        self
    }

    /// 设置查询超时时间，单位为毫秒
    pub fn timeout_ms(mut self, timeout_ms: u32) -> Self {
        self.timeout_ms = Some(timeout_ms);

        self
    }

    pub(crate) fn validate(&self) -> OtsResult<()> {
        if !validate_table_name(&self.table_name) {
            return Err(OtsError::ValidationFailed(format!("invalid table name: {}", self.table_name)));
        }

        if !validate_index_name(&self.index_name) {
            return Err(OtsError::ValidationFailed(format!("invalid index name: {}", self.index_name)));
        }

        if let Some(n) = self.timeout_ms {
            if n > i32::MAX as u32 {
                return Err(OtsError::ValidationFailed(format!("invalid timeout(ms): {}", n)));
            }
        }

        self.search_query.validate()?;

        Ok(())
    }
}

impl From<SearchRequest> for crate::protos::search::SearchRequest {
    fn from(value: SearchRequest) -> Self {
        let SearchRequest {
            table_name,
            index_name,
            search_query,
            routing_values,
            columns_to_get,
            column_return_type,
            timeout_ms,
        } = value;

        Self {
            table_name: Some(table_name),
            index_name: Some(index_name),
            columns_to_get: if !columns_to_get.is_empty() || column_return_type.is_some() {
                Some(crate::protos::search::ColumnsToGet {
                    return_type: column_return_type.map(|v| v as i32),
                    column_names: columns_to_get.into_iter().collect(),
                })
            } else {
                None
            },
            search_query: Some(crate::protos::search::SearchQuery::from(search_query).encode_to_vec()),
            routing_values: vec![],
            timeout_ms: timeout_ms.map(|n| n as i32),
        }
    }
}

/// 多元索引搜索
#[derive(Debug, Clone)]
pub struct SearchOperation {
    client: OtsClient,
    request: SearchRequest,
}

add_per_request_options!(SearchOperation);

impl SearchOperation {
    pub(crate) fn new(client: OtsClient, request: SearchRequest) -> Self {
        Self { client, request }
    }

    pub async fn send(self) -> OtsResult<()> {
        self.request.validate()?;

        let Self { client, request } = self;

        let msg = crate::protos::search::SearchRequest::from(request);

        let req = OtsRequest {
            operation: OtsOp::Search,
            body: msg.encode_to_vec(),
            ..Default::default()
        };

        let res = client.send(req).await?;

        let res = crate::protos::search::SearchResponse::decode(res.bytes().await?)?;

        log::debug!("{:#?}", res);

        Ok(())
    }
}
