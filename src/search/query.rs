use prost::Message;

use crate::{
    OtsResult,
    error::OtsError,
    protos::search::{Collapse, HighlightEncoder, HighlightFragmentOrder, QueryOperator, QueryType, SearchFilter},
    table::rules::validate_column_name,
};

use super::{Aggregation, GroupBy, Sort, Sorter};

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

    pub(crate) fn validate(&self) -> OtsResult<()> {
        if !validate_column_name(&self.field_name) {
            return Err(OtsError::ValidationFailed(format!("invalid field name: {}", self.field_name)));
        }

        Ok(())
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
            },
        }
    }
}

impl Query {
    pub(crate) fn validate(&self) -> OtsResult<()> {
        match self {
            Query::Match(mq) => mq.validate(),
        }
    }
}

/// HighlightParameter数据类型定义，表示高亮参数。
#[derive(Debug, Default, Clone)]
pub struct HighlightParameter {
    /// 字段名称。请确保在创建多元索引时已为该字段开启查询摘要与高亮。
    pub field_name: String,

    /// 返回高亮分片的最大数量，推荐设置为1。
    pub number_of_fragments: Option<u32>,

    /// 每个分片的长度
    pub fragment_size: Option<u32>,

    /// 查询词高亮的前置 Tag，例如 `<em>`、 `<b>`。
    /// 默认值为 `<em>`，您可以按需自定义前置 Tag。
    /// 支持的字符集包括 `< > " ' /、a-z、A-Z、0-9`。
    pub pre_tag: Option<String>,

    /// 查询词高亮的后置 Tag，例如 `</em>`、 `</b>`。
    /// 默认值为 `</em>`，您可以按需自定义后置Tag。
    /// 支持的字符集包括 `< > " ' /、a-z、A-Z、0-9`。
    pub post_tag: Option<String>,

    /// 当高亮字段返回多个分片时，分片的排序规则。
    pub fragments_order: Option<HighlightFragmentOrder>,
}

impl HighlightParameter {
    pub fn new(field_name: &str) -> Self {
        Self {
            field_name: field_name.to_string(),
            ..Default::default()
        }
    }

    /// 设置字段名称
    pub fn field_name(mut self, field_name: &str) -> Self {
        self.field_name = field_name.to_string();

        self
    }

    /// 设置返回分片的最大数量
    pub fn number_of_fragments(mut self, n: u32) -> Self {
        self.number_of_fragments = Some(n);

        self
    }

    /// 设置每个分片的长度
    pub fn fragment_size(mut self, n: u32) -> Self {
        self.fragment_size = Some(n);

        self
    }

    /// 设置开始标签
    pub fn pre_tag(mut self, pre_tag: impl Into<String>) -> Self {
        self.pre_tag = Some(pre_tag.into());

        self
    }

    /// 设置结束标签
    pub fn post_tag(mut self, post_tag: impl Into<String>) -> Self {
        self.post_tag = Some(post_tag.into());

        self
    }

    pub(crate) fn validate(&self) -> OtsResult<()> {
        if !validate_column_name(&self.field_name) {
            return Err(OtsError::ValidationFailed(format!("invalid highlight field name: {}", self.field_name)));
        }

        if let Some(n) = self.number_of_fragments {
            if n > i32::MAX as u32 {
                return Err(OtsError::ValidationFailed(format!("invalid number of fragments: {}", n)));
            }
        }

        if let Some(n) = self.fragment_size {
            if n > i32::MAX as u32 {
                return Err(OtsError::ValidationFailed(format!("invalid fragment size: {}", n)));
            }
        }

        Ok(())
    }
}

impl From<HighlightParameter> for crate::protos::search::HighlightParameter {
    fn from(value: HighlightParameter) -> Self {
        let HighlightParameter {
            field_name,
            number_of_fragments,
            fragment_size,
            pre_tag,
            post_tag,
            fragments_order,
        } = value;

        Self {
            field_name: Some(field_name),
            number_of_fragments: number_of_fragments.map(|n| n as i32),
            fragment_size: fragment_size.map(|n| n as i32),
            pre_tag,
            post_tag,
            fragments_order: fragments_order.map(|o| o as i32),
        }
    }
}

/// 查询摘要与高亮配置。
#[derive(Debug, Default, Clone)]
pub struct Highlight {
    /// 高亮参数。仅支持设置SearchQuery中包含关键词查询的字段。
    parameters: Vec<HighlightParameter>,

    /// 对高亮分片原文内容的编码方式。
    encoder: Option<HighlightEncoder>,
}

impl Highlight {
    pub fn new() -> Self {
        Self::default()
    }

    /// 添加一个高亮参数
    pub fn parameter(mut self, param: HighlightParameter) -> Self {
        self.parameters.push(param);

        self
    }

    /// 设置高亮参数
    pub fn parameters(mut self, params: impl IntoIterator<Item = HighlightParameter>) -> Self {
        self.parameters = params.into_iter().collect();

        self
    }

    /// 设置编码方式
    pub fn encoder(mut self, encoder: HighlightEncoder) -> Self {
        self.encoder = Some(encoder);

        self
    }

    pub(crate) fn validate(&self) -> OtsResult<()> {
        for p in &self.parameters {
            p.validate()?;
        }

        Ok(())
    }
}

impl From<Highlight> for crate::protos::search::Highlight {
    fn from(value: Highlight) -> Self {
        let Highlight { parameters, encoder } = value;

        Self {
            highlight_parameters: parameters.into_iter().map(|h| h.into()).collect(),
            highlight_encoder: encoder.map(|e| e as i32),
        }
    }
}

/// 多元索引数据查询配置
#[derive(Debug, Clone)]
pub struct SearchQuery {
    /// 查询条件。
    pub query: Query,

    /// 过滤器中的过滤条件
    pub filter: Option<Query>,

    /// 本次查询的开始位置。
    pub offset: Option<u32>,

    /// 本次查询需要返回的最大数量。
    pub limit: Option<u32>,

    /// 当符合查询条件的数据未读取完时，服务端会返回 `next_token`，此时可以使用 `next_token` 继续读取后面的数据。
    pub token: Vec<u8>,

    /// 按照指定列对返回结果进行去重。
    ///
    /// 按该列对结果集做折叠，只支持应用于整型、浮点数和 `Keyword` 类型的列，不支持数组类型的列。
    pub collapse_field_name: Option<String>,

    /// 返回结果的排序方式。
    pub sorters: Vec<Sorter>,

    /// 当指定非 PrimaryKeySort 的 sorter 时，默认情况下会主动添加 PrimaryKeySort，
    /// 通过该参数可禁止主动添加 PrimaryKeySort
    pub disable_default_pk_sorter: bool,

    /// 是否返回匹配的总行数，默认为 `false`，表示不返回。
    /// 返回匹配的总行数会影响查询性能。
    pub track_total_count: bool,

    /// 分组配置。
    pub group_bys: Vec<GroupBy>,

    /// 统计聚合配置。
    pub aggregations: Vec<Aggregation>,

    /// 查询摘要与高亮配置
    pub highlight: Option<Highlight>,
}

impl SearchQuery {
    pub fn new(query: Query) -> Self {
        Self {
            query,
            filter: None,
            offset: None,
            limit: None,
            token: vec![],
            collapse_field_name: None,
            sorters: vec![],
            disable_default_pk_sorter: false,
            track_total_count: false,
            group_bys: vec![],
            aggregations: vec![],
            highlight: None,
        }
    }

    /// 设置查询条件
    pub fn query(mut self, query: Query) -> Self {
        self.query = query;

        self
    }

    /// 设置查询中的过滤器
    pub fn filter(mut self, filter: Query) -> Self {
        self.filter = Some(filter);

        self
    }

    /// 设置偏移量
    pub fn offset(mut self, n: u32) -> Self {
        self.offset = Some(n);

        self
    }

    /// 设置本次查询需要返回的最大数量
    pub fn limit(mut self, n: u32) -> Self {
        self.limit = Some(n);

        self
    }

    /// 设置查询的游标
    pub fn token(mut self, token: impl Into<Vec<u8>>) -> Self {
        self.token = token.into();

        self
    }

    /// 设置对返回结果进行去重的列名
    pub fn collapse_field_name(mut self, field_name: impl Into<String>) -> Self {
        self.collapse_field_name = Some(field_name.into());

        self
    }

    /// 设置是否返回匹配的总行数
    pub fn track_total_count(mut self, total_count: bool) -> Self {
        self.track_total_count = total_count;

        self
    }

    /// 添加一个排序设置
    pub fn sorter(mut self, sorter: Sorter) -> Self {
        self.sorters.push(sorter);

        self
    }

    /// 设置排序
    pub fn sorters(mut self, sorters: impl IntoIterator<Item = Sorter>) -> Self {
        self.sorters = sorters.into_iter().collect();

        self
    }

    /// 设置是否禁用主动添加 PrimaryKeySort，
    pub fn disable_default_pk_sorter(mut self, disable_default_pk_sorter: bool) -> Self {
        self.disable_default_pk_sorter = disable_default_pk_sorter;

        self
    }

    /// 添加一个聚合配置
    pub fn aggregation(mut self, aggr: Aggregation) -> Self {
        self.aggregations.push(aggr);

        self
    }

    /// 设置聚合配置
    pub fn aggregations(mut self, aggrs: impl IntoIterator<Item = Aggregation>) -> Self {
        self.aggregations = aggrs.into_iter().collect();

        self
    }

    /// 添加一个分组配置
    pub fn group_by(mut self, group: GroupBy) -> Self {
        self.group_bys.push(group);

        self
    }

    /// 设置分组
    pub fn group_bys(mut self, groups: impl IntoIterator<Item = GroupBy>) -> Self {
        self.group_bys = groups.into_iter().collect();

        self
    }

    /// 设置高亮
    pub fn highlight(mut self, highlight: Highlight) -> Self {
        self.highlight = Some(highlight);

        self
    }

    pub(crate) fn validate(&self) -> OtsResult<()> {
        self.query.validate()?;

        if let Some(n) = self.offset {
            if n > i32::MAX as u32 {
                return Err(OtsError::ValidationFailed(format!("invalid offset: {}", n)));
            }
        }

        if let Some(n) = self.limit {
            if n > i32::MAX as u32 {
                return Err(OtsError::ValidationFailed(format!("invalid limit: {}", n)));
            }
        }

        if let Some(s) = &self.collapse_field_name {
            if !validate_column_name(s) {
                return Err(OtsError::ValidationFailed(format!("invalid collapse field name: {}", s)));
            }
        }

        if let Some(h) = &self.highlight {
            h.validate()?;
        }

        for s in &self.sorters {
            s.validate()?;
        }

        for g in &self.group_bys {
            g.validate()?;
        }

        for a in &self.aggregations {
            a.validate()?;
        }

        Ok(())
    }
}

impl From<SearchQuery> for crate::protos::search::SearchQuery {
    fn from(value: SearchQuery) -> Self {
        let SearchQuery {
            query,
            filter,
            offset,
            limit,
            token,
            collapse_field_name,
            sorters,
            disable_default_pk_sorter,
            track_total_count,
            group_bys,
            aggregations,
            highlight,
        } = value;

        let sort = crate::protos::search::Sort::from(Sort::with_sorters(sorters, disable_default_pk_sorter));

        Self {
            offset: offset.map(|n| n as i32),
            limit: limit.map(|n| n as i32),
            query: Some(query.into()),
            collapse: collapse_field_name.map(|f| Collapse { field_name: Some(f) }),
            sort: Some(sort),
            token: if !token.is_empty() { Some(token) } else { None },
            aggs: Some(aggregations.into()),
            group_bys: Some(group_bys.into()),
            highlight: highlight.map(|h| h.into()),
            track_total_count: if track_total_count { Some(i32::MAX) } else { Some(-1) }, // Copy from Java SDK
            filter: filter.map(|f| SearchFilter { query: Some(f.into()) }),
        }
    }
}
