use crate::{
    OtsClient, add_per_request_options,
    model::{Filter, Row},
    protos::table_store::{ReturnType, RowExistenceExpectation},
};

/// 插入数据到指定的行
///
/// 官方文档：<https://help.aliyun.com/zh/tablestore/developer-reference/putrow>
#[derive(Debug, Clone, Default)]
pub struct PutRowOperation {
    client: OtsClient,
    pub table_name: String,
    pub row: Row,
    pub row_condition: RowExistenceExpectation,
    pub column_condition: Option<Filter>,
    pub return_type: Option<ReturnType>,
    pub return_columns: Vec<String>,
    pub transaction_id: Option<String>,
}

add_per_request_options!(PutRowOperation);
