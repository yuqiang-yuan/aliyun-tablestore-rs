use std::io::Cursor;

use byteorder::ReadBytesExt;

use crate::{OtsResult, crc8::crc_u8, error::OtsError, protos::plain_buffer};

use super::{DefinedColumn, PrimaryKeyColumn};

/// Row for wide-column table
#[derive(Debug, Clone, Default)]
pub struct Row {
    pub primary_keys: Vec<PrimaryKeyColumn>,
    pub columns: Vec<DefinedColumn>,
}

#[derive(Copy, Clone)]
enum RowType {
    PrimaryKey,
    Column,
}

impl Row {
    pub fn from_cursor(cursor: &mut Cursor<Vec<u8>>) -> OtsResult<Self> {
        let mut row_type: RowType = RowType::PrimaryKey;
        let mut primary_keys = vec![];
        let mut columns = vec![];

        loop {
            let tag = cursor.read_u8()?;
            log::debug!("tag = 0x{:02X}, pos = 0x{:02X}, len = 0x{:02X}", tag, cursor.position(), cursor.get_ref().len());
            if cursor.position() as usize >= cursor.get_ref().len() - 1 {
                log::debug!("read to stream end");
                break;
            }

            match tag {
                plain_buffer::TAG_ROW_PK => {
                    row_type = RowType::PrimaryKey;
                }

                plain_buffer::TAG_ROW_DATA => {
                    row_type = RowType::Column;
                }

                plain_buffer::TAG_CELL => match row_type {
                    RowType::PrimaryKey => {
                        let pkc = PrimaryKeyColumn::from_cursor(cursor)?;
                        primary_keys.push(pkc);
                    }

                    RowType::Column => {
                        let cell = DefinedColumn::from_cursor(cursor)?;
                        columns.push(cell);
                    }
                },

                plain_buffer::TAG_ROW_CHECKSUM => {
                    let checksum = cursor.read_u8()?;

                    match row_type {
                        RowType::PrimaryKey => {
                            let mut row_checksum = 0u8;
                            for key_col in &primary_keys {
                                row_checksum = crc_u8(row_checksum, key_col.crc8_checksum());
                            }

                            if row_checksum != checksum {
                                return Err(OtsError::PlainBufferError(format!(
                                    "primary key row checksum validation failed. calculated: {}, received: {}",
                                    row_checksum, checksum
                                )));
                            }
                        }
                        RowType::Column => {
                            let mut row_checksum = 0u8;
                            for col in &columns {
                                row_checksum = crc_u8(row_checksum, col.crc8_checksum());
                            }

                            if row_checksum != checksum {
                                return Err(OtsError::PlainBufferError(format!(
                                    "data row checksum validation failed. calculated: {}, received: {}",
                                    row_checksum, checksum
                                )));
                            }
                        }
                    }
                    break;
                }

                _ => return Err(OtsError::PlainBufferError(format!("invalid tag: {}", tag))),
            };
        }

        Ok(Self { primary_keys, columns })
    }
}
