// This file is @generated by prost-build.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValueTransferRule {
    #[prost(string, required, tag = "1")]
    pub regex: ::prost::alloc::string::String,
    #[prost(enumeration = "VariantType", optional, tag = "2")]
    pub cast_type: ::core::option::Option<i32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SingleColumnValueFilter {
    #[prost(enumeration = "ComparatorType", required, tag = "1")]
    pub comparator: i32,
    #[prost(string, required, tag = "2")]
    pub column_name: ::prost::alloc::string::String,
    #[prost(bytes = "vec", required, tag = "3")]
    pub column_value: ::prost::alloc::vec::Vec<u8>,
    #[prost(bool, required, tag = "4")]
    pub filter_if_missing: bool,
    #[prost(bool, required, tag = "5")]
    pub latest_version_only: bool,
    #[prost(message, optional, tag = "6")]
    pub value_trans_rule: ::core::option::Option<ValueTransferRule>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CompositeColumnValueFilter {
    #[prost(enumeration = "LogicalOperator", required, tag = "1")]
    pub combinator: i32,
    #[prost(message, repeated, tag = "2")]
    pub sub_filters: ::prost::alloc::vec::Vec<Filter>,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct ColumnPaginationFilter {
    #[prost(int32, required, tag = "1")]
    pub offset: i32,
    #[prost(int32, required, tag = "2")]
    pub limit: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Filter {
    #[prost(enumeration = "FilterType", required, tag = "1")]
    pub r#type: i32,
    /// Serialized string of filter of the type
    #[prost(bytes = "vec", required, tag = "2")]
    pub filter: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum VariantType {
    VtInteger = 0,
    VtDouble = 1,
    /// VT_BOOLEAN = 2;
    VtString = 3,
    VtNull = 6,
    VtBlob = 7,
}
impl VariantType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::VtInteger => "VT_INTEGER",
            Self::VtDouble => "VT_DOUBLE",
            Self::VtString => "VT_STRING",
            Self::VtNull => "VT_NULL",
            Self::VtBlob => "VT_BLOB",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "VT_INTEGER" => Some(Self::VtInteger),
            "VT_DOUBLE" => Some(Self::VtDouble),
            "VT_STRING" => Some(Self::VtString),
            "VT_NULL" => Some(Self::VtNull),
            "VT_BLOB" => Some(Self::VtBlob),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum FilterType {
    FtSingleColumnValue = 1,
    FtCompositeColumnValue = 2,
    FtColumnPagination = 3,
}
impl FilterType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::FtSingleColumnValue => "FT_SINGLE_COLUMN_VALUE",
            Self::FtCompositeColumnValue => "FT_COMPOSITE_COLUMN_VALUE",
            Self::FtColumnPagination => "FT_COLUMN_PAGINATION",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "FT_SINGLE_COLUMN_VALUE" => Some(Self::FtSingleColumnValue),
            "FT_COMPOSITE_COLUMN_VALUE" => Some(Self::FtCompositeColumnValue),
            "FT_COLUMN_PAGINATION" => Some(Self::FtColumnPagination),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ComparatorType {
    CtEqual = 1,
    CtNotEqual = 2,
    CtGreaterThan = 3,
    CtGreaterEqual = 4,
    CtLessThan = 5,
    CtLessEqual = 6,
    CtExist = 7,
    CtNotExist = 8,
}
impl ComparatorType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::CtEqual => "CT_EQUAL",
            Self::CtNotEqual => "CT_NOT_EQUAL",
            Self::CtGreaterThan => "CT_GREATER_THAN",
            Self::CtGreaterEqual => "CT_GREATER_EQUAL",
            Self::CtLessThan => "CT_LESS_THAN",
            Self::CtLessEqual => "CT_LESS_EQUAL",
            Self::CtExist => "CT_EXIST",
            Self::CtNotExist => "CT_NOT_EXIST",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "CT_EQUAL" => Some(Self::CtEqual),
            "CT_NOT_EQUAL" => Some(Self::CtNotEqual),
            "CT_GREATER_THAN" => Some(Self::CtGreaterThan),
            "CT_GREATER_EQUAL" => Some(Self::CtGreaterEqual),
            "CT_LESS_THAN" => Some(Self::CtLessThan),
            "CT_LESS_EQUAL" => Some(Self::CtLessEqual),
            "CT_EXIST" => Some(Self::CtExist),
            "CT_NOT_EXIST" => Some(Self::CtNotExist),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum LogicalOperator {
    LoNot = 1,
    LoAnd = 2,
    LoOr = 3,
}
impl LogicalOperator {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::LoNot => "LO_NOT",
            Self::LoAnd => "LO_AND",
            Self::LoOr => "LO_OR",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "LO_NOT" => Some(Self::LoNot),
            "LO_AND" => Some(Self::LoAnd),
            "LO_OR" => Some(Self::LoOr),
            _ => None,
        }
    }
}
