
use prost::Message;

use crate::protos::search::{QueryOperator, QueryType};

/// 包括模糊匹配和短语或邻近查询
#[derive(Debug, Default, Clone)]
pub struct MatchQuery {
    /// 要匹配的字段
    pub field_name: String,

    /// 模糊匹配的值
    ///
    /// 当要匹配的列为 `Text` 类型时，查询关键词会被分词成多个词，分词类型为创建多元索引时设置的分词器类型。如果创建多元索引时未设置分词器类型，则默认分词类型为单字分词。
    /// 例如当要匹配的列为 `Text` 类型时，分词类型为单字分词。例如查询词为 `"this is"`，可以匹配到:
    ///
    /// - `"..., this is tablestore"`
    /// - `"is this tablestore"`
    /// - `"tablestore is cool"`
    /// - `"this"`
    /// - `"is"`
    /// - 等
    pub text: String,

    /// 最小匹配个数，必须与逻辑运算符 `OR` 配合使用。
    /// 只有当某一行数据的 `field_name` 列的值中至少包括最小匹配个数的词时，才会返回该行数据。
    pub minimum_should_match: Option<u32>,

    /// 查询操作符。取值范围为逻辑运算符 `AND` 和 `OR`。
    /// 默认值为 `OR`，表示当分词后的多个词只要有部分匹配时，则行数据满足查询条件。
    pub operator: Option<QueryOperator>,

    /// 查询条件的权重配置。
    pub weight: Option<f32>,
}

impl MatchQuery {
    pub fn new(field_name: &str, text: impl Into<String>) -> Self {
        Self {
            field_name: field_name.to_string(),
            text: text.into(),
            ..Default::default()
        }
    }

    /// 设置查询列名
    pub fn field_name(mut self, field_name: &str) -> Self {
        self.field_name = field_name.to_string();

        self
    }

    /// 设置查询值
    pub fn text(mut self, text: impl Into<String>) -> Self {
        self.text = text.into();

        self
    }

    /// 设置最小匹配个数
    pub fn minimum_should_match(mut self, min_should_match: u32) -> Self {
        self.minimum_should_match = Some(min_should_match);

        self
    }

    /// 设置查询操作符
    pub fn operator(mut self, operator: QueryOperator) -> Self {
        self.operator = Some(operator);

        self
    }

    /// 设置查询权重
    pub fn weight(mut self, weight: f32) -> Self {
        self.weight = Some(weight);

        self
    }
}

impl From<MatchQuery> for crate::protos::search::MatchQuery {
    fn from(value: MatchQuery) -> Self {
        let MatchQuery {
            field_name,
            text,
            minimum_should_match,
            operator,
            weight,
        } = value;

        Self {
            field_name: Some(field_name),
            text: Some(text),
            minimum_should_match: minimum_should_match.map(|v| v as i32),
            operator: operator.map(|o| o as i32),
            weight,
        }
    }
}

/// 多元索引查询条件枚举
#[derive(Debug, Clone)]
pub enum Query {
    Match(MatchQuery),
}

impl From<Query> for crate::protos::search::Query {
    fn from(value: Query) -> Self {
        match value {
            Query::Match(mq) => Self {
                r#type: Some(QueryType::MatchQuery as i32),
                query: Some(crate::protos::search::MatchQuery::from(mq).encode_to_vec()),
            }
        }
    }
}
