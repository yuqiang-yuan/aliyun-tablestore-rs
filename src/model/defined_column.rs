use std::io::{Cursor, Read};

use byteorder::{LittleEndian, ReadBytesExt};

use crate::{
    OtsResult,
    crc8::{crc_bytes, crc_f64, crc_i64, crc_u8, crc_u32, crc_u64},
    error::OtsError,
    protos::plain_buffer::{self, VT_BLOB, VT_BOOLEAN, VT_DOUBLE, VT_INF_MAX, VT_INF_MIN, VT_INTEGER, VT_NULL, VT_STRING},
};

#[derive(Debug, Clone, Default, PartialEq)]
pub enum CellValue {
    #[default]
    Null,
    Integer(i64),
    Double(f64),
    Boolean(bool),
    String(String),
    Blob(Vec<u8>),
    InfMin,
    InfMax,
}

impl CellValue {
    /// Calculate the cell checksum
    pub(crate) fn crc8_checksum(&self, input_checksum: u8) -> u8 {
        let mut checksum = input_checksum;

        match self {
            Self::Null => crc_u8(checksum, VT_NULL),
            Self::InfMin => crc_u8(checksum, VT_INF_MIN),
            Self::InfMax => crc_u8(checksum, VT_INF_MAX),

            Self::Integer(n) => {
                checksum = crc_u8(checksum, VT_INTEGER);
                crc_i64(checksum, *n)
            }

            Self::Double(d) => {
                checksum = crc_u8(checksum, VT_DOUBLE);
                crc_f64(checksum, *d)
            }

            Self::Boolean(b) => {
                checksum = crc_u8(checksum, VT_BOOLEAN);
                crc_u8(checksum, if *b { 1u8 } else { 0u8 })
            }

            Self::String(s) => {
                checksum = crc_u8(checksum, VT_STRING);
                checksum = crc_u32(checksum, s.len() as u32);
                crc_bytes(checksum, s.as_bytes())
            }

            Self::Blob(buf) => {
                checksum = crc_u8(checksum, VT_BLOB);
                checksum = crc_u32(checksum, buf.len() as u32);
                crc_bytes(checksum, buf)
            }
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct DefinedColumn {
    pub name: String,
    pub value: CellValue,
    pub timestamp: Option<u64>,
}

impl DefinedColumn {
    pub(crate) fn from_cursor(cursor: &mut Cursor<Vec<u8>>) -> OtsResult<Self> {
        let mut name = String::new();
        let mut value = CellValue::Integer(0);
        let mut checksum = 0u8;
        let mut ts: Option<u64> = None;

        loop {
            if cursor.position() >= (cursor.get_ref().len() - 1) as u64 {
                break;
            }

            let tag = cursor.read_u8()?;

            match tag {
                plain_buffer::TAG_CELL_NAME => {
                    let len = cursor.read_u32::<LittleEndian>()? as usize;
                    let mut buf: Vec<u8> = vec![0u8; len];

                    cursor.read_exact(&mut buf)?;
                    name = String::from_utf8(buf)?;
                }

                plain_buffer::TAG_CELL_VALUE => {
                    let _previx = cursor.read_u32::<LittleEndian>()?;
                    let cell_value_type = cursor.read_u8()?;

                    value = match cell_value_type {
                        plain_buffer::VT_INTEGER => CellValue::Integer(cursor.read_i64::<LittleEndian>()?),

                        plain_buffer::VT_DOUBLE => CellValue::Double(cursor.read_f64::<LittleEndian>()?),

                        plain_buffer::VT_BOOLEAN => {
                            let b = cursor.read_u8()?;
                            CellValue::Boolean(b == 0x01)
                        }

                        plain_buffer::VT_STRING => {
                            let len = cursor.read_u32::<LittleEndian>()? as usize;
                            let mut buf: Vec<u8> = vec![0u8; len];
                            cursor.read_exact(&mut buf)?;
                            CellValue::String(String::from_utf8(buf)?)
                        }

                        plain_buffer::VT_BLOB => {
                            let len = cursor.read_u32::<LittleEndian>()? as usize;
                            let mut buf: Vec<u8> = vec![0u8; len];
                            cursor.read_exact(&mut buf)?;
                            CellValue::Blob(buf)
                        }

                        _ => return Err(OtsError::PlainBufferError(format!("unknown data row cell value type: {}", cell_value_type))),
                    };
                }

                plain_buffer::TAG_CELL_TIMESTAMP => {
                    ts = Some(cursor.read_u64::<LittleEndian>()?);
                }

                plain_buffer::TAG_CELL_CHECKSUM => {
                    checksum = cursor.read_u8()?;
                    break;
                }

                _ => return Err(OtsError::PlainBufferError(format!("unknown tag: {}", tag))),
            }
        }

        let col = Self { name, value, timestamp: ts };

        let cell_checksum = col.crc8_checksum();

        // log::debug!("cell {}, calculated checksum {}, received checksum {}", col.name, cell_checksum, checksum);

        if cell_checksum != checksum {
            return Err(OtsError::PlainBufferError(format!(
                "data row cell checksum validation failed. calculated: {}, received: {}",
                cell_checksum, checksum
            )));
        }

        Ok(col)
    }

    pub(crate) fn crc8_checksum(&self) -> u8 {
        let mut cell_checksum = 0u8;
        cell_checksum = crc_bytes(cell_checksum, self.name.as_bytes());
        cell_checksum = self.value.crc8_checksum(cell_checksum);
        if let Some(d) = &self.timestamp {
            cell_checksum = crc_u64(cell_checksum, *d);
        }
        cell_checksum
    }

    pub fn with_integer_value(name: &str, value: i64) -> Self {
        Self {
            name: name.to_string(),
            value: CellValue::Integer(value),
            ..Default::default()
        }
    }

    pub fn with_double_value(name: &str, value: f64) -> Self {
        Self {
            name: name.to_string(),
            value: CellValue::Double(value),
            ..Default::default()
        }
    }

    pub fn with_bool_value(name: &str, value: bool) -> Self {
        Self {
            name: name.to_string(),
            value: CellValue::Boolean(value),
            ..Default::default()
        }
    }

    pub fn with_string_value(name: &str, value: impl Into<String>) -> Self {
        Self {
            name: name.to_string(),
            value: CellValue::String(value.into()),
            ..Default::default()
        }
    }

    pub fn with_blob_value(name: &str, value: impl Into<Vec<u8>>) -> Self {
        Self {
            name: name.to_string(),
            value: CellValue::Blob(value.into()),
            ..Default::default()
        }
    }

    pub fn with_null(name: &str) -> Self {
        Self {
            name: name.to_string(),
            value: CellValue::Null,
            ..Default::default()
        }
    }

    pub fn with_infinite_min(name: &str) -> Self {
        Self {
            name: name.to_string(),
            value: CellValue::InfMin,
            ..Default::default()
        }
    }

    pub fn with_infinite_max(name: &str) -> Self {
        Self {
            name: name.to_string(),
            value: CellValue::InfMax,
            ..Default::default()
        }
    }
}
