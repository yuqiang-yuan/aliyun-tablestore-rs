//! Aliyun plain buffer. See <https://help.aliyun.com/zh/tablestore/developer-reference/plainbuffer> for more details.

pub const LITTLE_ENDIAN_32_SIZE: u32 = 4;
pub const LITTLE_ENDIAN_64_SIZE: u32 = 8;

pub const HEADER: u32 = 0x75;

// tag types
pub const TAG_ROW_PK: u8 = 0x01;
pub const TAG_ROW_DATA: u8 = 0x02;
pub const TAG_CELL: u8 = 0x03;
pub const TAG_CELL_NAME: u8 = 0x04;
pub const TAG_CELL_VALUE: u8 = 0x05;
pub const TAG_CELL_TYPE: u8 = 0x06;
pub const TAG_CELL_TIMESTAMP: u8 = 0x07;
pub const TAG_DELETE_ROW_MARKER: u8 = 0x08;
pub const TAG_ROW_CHECKSUM: u8 = 0x09;
pub const TAG_CELL_CHECKSUM: u8 = 0x0A;
pub const TAG_EXTENSION: u8 = 0x0B;
pub const TAG_SEQ_INFO: u8 = 0x0C;
pub const TAG_SEQ_INFO_EPOCH: u8 = 0x0D;
pub const TAG_SEQ_INFO_TS: u8 = 0x0E;
pub const TAG_SEQ_INFO_ROW_INDEX: u8 = 0x0F;

// cell operation types
pub const DELETE_ALL_VERSION: u8 = 0x01;
pub const DELETE_ONE_VERSION: u8 = 0x03;
pub const INCREMENT: u8 = 0x04;

// variant types
pub const VT_INTEGER: u8 = 0x00;
pub const VT_DOUBLE: u8 = 0x01;
pub const VT_BOOLEAN: u8 = 0x02;
pub const VT_STRING: u8 = 0x03;
pub const VT_NULL: u8 = 0x06;
pub const VT_BLOB: u8 = 0x07;
pub const VT_INF_MIN: u8 = 0x09;
pub const VT_INF_MAX: u8 = 0x0A;
pub const VT_AUTO_INCREMENT: u8 = 0x0B;
