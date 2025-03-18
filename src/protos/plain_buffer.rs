//! Aliyun plain buffer. See <https://help.aliyun.com/zh/tablestore/developer-reference/plainbuffer> for more details.
//!
//! - plain buffer 中涉及到数值的，包括整数、浮点数，都是用小端序（Little Endian）排列。
//! - plain buffer 中涉及到字符串的，都是用 UTF-8 编码表示。
//! - plain buffer 中的 Cell 是指：
//!   - 主键中的一个列和其值的组合
//!   - 数据中的一个列和其值的组合
//!
//! | Value | Bytes | Description |
//! | ----  | ----- | ----------- |
//! | `0x75u32` | 4 | HEADER |
//! | `0x01u8`| 1 | TAG_ROW_PK |
//! | `0x03u8` | 1 | TAG_CELL |
//! | `0x04u8` | 1 | TAG_CELL_NAME |
//! | `<name-len>u32` | 4 | cell name length |
//! | `<name-bytes>` | variant length | cell name |
//! | `0x05u8` | 1 | TAG_CELL_VALUE |
//! | `<prefix>u32` | 4 | cell value prefix |
//! | `<variant>u8` | 1 | cell value type. See the following `VT_` constants |
//! | `<variant>u32` | 4 | cell value length. **optional** |
//! | `<value-bytes>` | variant length | cell value. **optional** |
//! | `0x0Au8` | 1 | TAG_CELL_CHECKSUM |
//! | `<variant>u8` | 1 | cell checksum |
//! | `0x09u8` | 1 | TAG_ROW_CHECKSUM |
//! | `<variant>u8` | 1 | row checksum |
//! | `0x02u8` | 1 | TAG_ROW_DATA |
//! | ... | ... | 循环 TAG_CELL 到 cell value |
//! | `0x01u8` or `0x03u8` or `0x04u8`  | 1 | cell op. DELETE_ALL_VERSION, DELETE_ONE_VERSION or INCREMENT. **optional** |
//! | `0x07u8` | 1 | TAG_CELL_TIMESTAMP. **optional** |
//! | `<variant>u64` | 8 | cell timestamp value. **optional** |
//! | `0x09u8` | 1 | TAG_ROW_CHECKSUM |
//! | `<variant>u8` | 1 | row checksum |
//!
//! cell value prefix 实际上是指整个 cell 值（不包含 CRC 校验码部分）占多少字节
//!
//! - 整数及双精：4 字节前缀 + 1 字节类型 + 8 字节数据 = 13 = 0x0D
//! - 字符串：4 字节前缀 + 1 字节类型 + 4 字节长度 + 内容长度
//! - BLOB: 4 字节前缀 + 1 字节类型 + 4 字节长度 + 内容长度
//! - 布尔值：4 字节前缀 + 1 字节类型 + 1 字节值
//! - InfMin, InfMax: 4 字节前缀 + 1 字节类型 = 5 = 0x05
//!

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
