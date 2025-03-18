use std::io::Cursor;

use byteorder::ReadBytesExt;

use crate::{OtsResult, crc8::crc_u8, error::OtsError, protos::plain_buffer};

use super::{CellValue, DefinedColumn, PrimaryKeyColumn, PrimaryKeyValue};

/// 宽表模型的行
#[derive(Debug, Clone, Default)]
pub struct Row {
    /// 主键列
    pub primary_keys: Vec<PrimaryKeyColumn>,

    /// 数据列
    pub columns: Vec<DefinedColumn>,
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum RowType {
    PrimaryKey,
    Column,
}

impl Row {
    /// 获取给定名称的主键的值
    pub fn get_primary_key_value(&self, name: &str) -> Option<&PrimaryKeyValue> {
        self.primary_keys.iter().find(|pk| pk.name.as_str() == name).map(|col| &col.value)
    }

    /// 获取给定名称的列的值, 适用于列在行中只出现一次的情况
    pub fn get_column_value(&self, name: &str) -> Option<&CellValue> {
        self.columns.iter().find(|c| c.name.as_str() == name).map(|c| &c.value)
    }

    /// 从 cursor 构建行
    pub(crate) fn from_cursor(cursor: &mut Cursor<Vec<u8>>) -> OtsResult<Self> {
        let mut row_type: RowType = RowType::PrimaryKey;
        let mut primary_keys = vec![];
        let mut columns = vec![];

        loop {
            let tag = cursor.read_u8()?;
            // log::debug!("tag = 0x{:02X}, pos = 0x{:02X}, len = 0x{:02X}", tag, cursor.position(), cursor.get_ref().len());
            if cursor.position() as usize >= cursor.get_ref().len() - 1 {
                // log::debug!("read to stream end");
                break;
            }

            match tag {
                plain_buffer::TAG_ROW_PK => {
                    // log::debug!("TAG_ROW_PK read");
                    row_type = RowType::PrimaryKey;
                }

                plain_buffer::TAG_ROW_DATA => {
                    // log::debug!("TAG_ROW_DATA read");
                    row_type = RowType::Column;
                }

                plain_buffer::TAG_CELL => match row_type {
                    RowType::PrimaryKey => {
                        let pkc = PrimaryKeyColumn::from_cursor(cursor)?;
                        // log::debug!("primary key column read: {:#?}", pkc);
                        primary_keys.push(pkc);
                    }

                    RowType::Column => {
                        let cell = DefinedColumn::from_cursor(cursor)?;
                        // log::debug!("data column read: {:#?}", cell);
                        columns.push(cell);
                    }
                },

                plain_buffer::TAG_ROW_CHECKSUM => {
                    // log::debug!("TAG_ROW_CHECKSUM read");
                    let checksum = cursor.read_u8()?;

                    let mut row_checksum = 0u8;
                    for key_col in &primary_keys {
                        // log::debug!("primary key: {:#?}", key_col);
                        row_checksum = crc_u8(row_checksum, key_col.crc8_checksum());
                    }

                    for col in &columns {
                        // log::debug!("column: {:#?}", col);
                        row_checksum = crc_u8(row_checksum, col.crc8_checksum());
                    }

                    row_checksum = crc_u8(row_checksum, 0u8);

                    if row_checksum != checksum {
                        return Err(OtsError::PlainBufferError(format!(
                            "data row checksum validation failed. calculated: {}, received: {}",
                            row_checksum, checksum
                        )));
                    }
                    break;
                }

                _ => return Err(OtsError::PlainBufferError(format!("invalid tag: {}", tag))),
            };
        }

        Ok(Self { primary_keys, columns })
    }
}
