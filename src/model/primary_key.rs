use std::io::{Cursor, Read, Write};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::{
    OtsResult,
    crc8::{crc_bytes, crc_i64, crc_u8, crc_u32},
    error::OtsError,
    protos::plain_buffer::{
        self, HEADER, LITTLE_ENDIAN_32_SIZE, LITTLE_ENDIAN_64_SIZE, TAG_CELL, TAG_CELL_CHECKSUM, TAG_CELL_NAME, TAG_CELL_VALUE, TAG_ROW_CHECKSUM, VT_BLOB,
        VT_INF_MAX, VT_INF_MIN, VT_INTEGER, VT_STRING,
    },
};

#[derive(Debug, Clone)]
pub struct PrimaryKey {
    pub keys: Vec<PrimaryKeyColumn>,
}

impl PrimaryKey {
    /// 0x03 - TAG_ROW_CELL
    /// 0x00 ... Keys size
    pub(crate) fn compute_size(&self) -> u32 {
        1u32 + self.keys.iter().map(|k| k.compute_size()).sum::<u32>()
    }

    /// 0x75 0x00 0x00 0x00    - header: 4 bytes
    /// 0x01                   - TAG_ROW_PK
    /// 0x00 ...               - Keys keysize with TAG_ROW_CELL
    /// 0x09                   - TAG_ROW_CHECKSUM
    /// 0x00                   - Checksum: 1 byte
    pub(crate) fn compute_size_with_header(&self) -> u32 {
        LITTLE_ENDIAN_32_SIZE + self.compute_size() + 2
    }

    /// Consume self and output plain buffer data
    pub(crate) fn into_plain_buffer(self, with_header: bool) -> Vec<u8> {
        let size = if with_header { self.compute_size_with_header() } else { self.compute_size() } as usize;

        let bytes = Vec::<u8>::with_capacity(size);
        let mut cursor = Cursor::new(bytes);

        if with_header {
            cursor.write_u32::<LittleEndian>(HEADER).unwrap();
            cursor.write_u8(plain_buffer::TAG_ROW_PK).unwrap();
        }

        self.write_plain_buffer(&mut cursor);

        cursor.into_inner()
    }

    /// Write data to cursor
    pub(crate) fn write_plain_buffer(self, cursor: &mut Cursor<Vec<u8>>) {
        let Self { keys } = self;

        let mut row_checksum = 0u8;
        for key_col in keys {
            cursor.write_u8(TAG_CELL).unwrap();
            let cell_checksum = key_col.write_plain_buffer(cursor);
            row_checksum = crc_u8(row_checksum, cell_checksum);
        }

        row_checksum = crc_u8(row_checksum, 0u8);
        cursor.write_u8(TAG_ROW_CHECKSUM).unwrap();
        cursor.write_u8(row_checksum).unwrap();
    }
}

#[derive(Debug, Clone)]
pub enum PrimaryKeyValue {
    Integer(i64),
    String(String),
    Binary(Vec<u8>),
    InfMax,
    InfMin,
    // AutoIncrement,
}

impl Default for PrimaryKeyValue {
    fn default() -> Self {
        Self::Integer(0)
    }
}

impl PrimaryKeyValue {
    /// 0x00 0x00 0x00 0x00 - Marker?, 4 bytes le
    ///                     - Integer: 0x08 + 0x01 = 0x09
    ///                     - String:  0x04 + 0x01 + string bytes len
    ///                     - Binary:  0x04 + 0x01 + bytes len
    ///                     - Inf min: 0x01
    ///                     - Inf max: 0x01
    /// 0x00                - Cell value type, 1 byte
    ///                     - Integer: 0x00
    ///                     - String:  0x03
    ///                     - Binary:  0x07
    ///                     - Inf min: 0x09, for query
    ///                     - Inf max: 0x0A, for query
    /// 0x00 ...            - Cell value
    ///                     - Integer: 8 bytes
    ///                     - String:  string utf8 bytes
    ///                     - Binary:  bytes
    ///                     - Inf min: 1 byte
    ///                     - Inf max: 1 byte
    pub(crate) fn compute_size(&self) -> u32 {
        let size = plain_buffer::LITTLE_ENDIAN_32_SIZE + 1;

        match self {
            Self::Integer(_) => size + LITTLE_ENDIAN_64_SIZE,
            Self::String(s) => size + plain_buffer::LITTLE_ENDIAN_32_SIZE + s.len() as u32,
            Self::Binary(buf) => size + plain_buffer::LITTLE_ENDIAN_32_SIZE + buf.len() as u32,
            Self::InfMax | Self::InfMin => size,
        }
    }

    pub(crate) fn crc8_checksum(&self, input_checksum: u8) -> u8 {
        let mut checksum = input_checksum;

        match self {
            Self::InfMin => crc_u8(checksum, VT_INF_MIN),
            Self::InfMax => crc_u8(checksum, VT_INF_MAX),
            // Self::AutoIncrement => crc_u8(crc, VT_AUTO_INCREMENT),
            Self::Integer(n) => {
                checksum = crc_u8(checksum, VT_INTEGER);
                crc_i64(checksum, *n)
            }

            Self::String(s) => {
                checksum = crc_u8(checksum, VT_STRING);
                checksum = crc_u32(checksum, s.len() as u32);
                crc_bytes(checksum, s.as_bytes())
            }

            Self::Binary(buf) => {
                checksum = crc_u8(checksum, VT_BLOB);
                checksum = crc_u32(checksum, buf.len() as u32);
                crc_bytes(checksum, buf)
            }
        }
    }

    /// Consume self and write plain buffer.
    pub(crate) fn write_plain_buffer(self, cursor: &mut Cursor<Vec<u8>>) {
        match self {
            Self::Integer(n) => {
                cursor.write_u32::<LittleEndian>(LITTLE_ENDIAN_64_SIZE + 1).unwrap();
                cursor.write_u8(VT_INTEGER).unwrap();
                cursor.write_i64::<LittleEndian>(n).unwrap();
            }

            Self::String(s) => {
                cursor.write_u32::<LittleEndian>(1 + LITTLE_ENDIAN_32_SIZE + s.len() as u32).unwrap();
                cursor.write_u8(VT_STRING).unwrap();
                cursor.write_u32::<LittleEndian>(s.len() as u32).unwrap();
                cursor.write_all(s.as_bytes()).unwrap();
            }

            Self::Binary(buf) => {
                cursor.write_u32::<LittleEndian>(1 + LITTLE_ENDIAN_32_SIZE + buf.len() as u32).unwrap();
                cursor.write_u8(VT_BLOB).unwrap();
                cursor.write_u32::<LittleEndian>(buf.len() as u32).unwrap();
                cursor.write_all(&buf).unwrap();
            }

            Self::InfMin => {
                cursor.write_u32::<LittleEndian>(1).unwrap();
                cursor.write_u8(VT_INF_MIN).unwrap();
            }

            Self::InfMax => {
                cursor.write_u32::<LittleEndian>(1).unwrap();
                cursor.write_u8(VT_INF_MAX).unwrap();
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PrimaryKeyColumn {
    pub name: String,
    pub value: PrimaryKeyValue,

    /// This is for parsing data crc check.
    pub(crate) checksum: Option<u8>,
}

impl PrimaryKeyColumn {
    /// Read the cursor from TAG_CELL_NAME. that means HEADER, TAG_ROW_PK has been read
    pub(crate) fn from_cursor(cursor: &mut Cursor<Vec<u8>>) -> OtsResult<Self> {
        let mut name = String::new();
        let mut value = PrimaryKeyValue::Integer(0);
        let mut checksum = 0u8;

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
                    // I don't know how to use this value
                    let _marker = cursor.read_u32::<LittleEndian>()?;
                    let cell_value_type = cursor.read_u8()?;

                    value = match cell_value_type {
                        plain_buffer::VT_INTEGER => PrimaryKeyValue::Integer(cursor.read_i64::<LittleEndian>()?),

                        plain_buffer::VT_STRING => {
                            let len = cursor.read_u32::<LittleEndian>()? as usize;
                            let mut buf: Vec<u8> = vec![0u8; len];

                            cursor.read_exact(&mut buf)?;
                            PrimaryKeyValue::String(String::from_utf8(buf)?)
                        }

                        plain_buffer::VT_BLOB => {
                            let len = cursor.read_u32::<LittleEndian>()? as usize;
                            let mut buf: Vec<u8> = vec![0u8; len];

                            cursor.read_exact(&mut buf)?;
                            PrimaryKeyValue::Binary(buf)
                        }

                        _ => return Err(OtsError::PlainBufferError(format!("unknown primary key cell value type: {}", cell_value_type))),
                    };
                }

                plain_buffer::TAG_CELL_CHECKSUM => {
                    checksum = cursor.read_u8()?;
                    break;
                }

                _ => return Err(OtsError::PlainBufferError(format!("unknown tag: {}", tag))),
            }
        }

        let pk_col = Self {
            name,
            value,
            checksum: Some(checksum),
        };

        let cell_checksum = pk_col.crc8_checksum();

        if cell_checksum != checksum {
            return Err(OtsError::PlainBufferError(format!(
                "primary key cell checksum validation failed. calculated: {}, received: {}",
                cell_checksum, checksum
            )));
        }

        Ok(pk_col)
    }

    pub(crate) fn crc8_checksum(&self) -> u8 {
        let mut cell_checksum = 0u8;
        cell_checksum = crc_bytes(cell_checksum, self.name.as_bytes());
        self.value.crc8_checksum(cell_checksum)
    }

    /// 0x03                - TAG_CELL
    /// 0x04                - TAG_CELL_NAME
    /// 0x00 0x00 0x00 0x00 - Cell name length u32, le
    /// 0x00 ...            - Cell name utf8 bytes
    /// 0x05                - TAG_CELL_VALUE
    /// 0x00 ...            - Cell value
    /// 0x0A                - TAG_CELL_CHECKSUM
    /// 0x00                - Cell checksum value
    pub(crate) fn compute_size(&self) -> u32 {
        2u32 + plain_buffer::LITTLE_ENDIAN_32_SIZE + self.name.len() as u32 + 1 + self.value.compute_size() + 2
    }

    pub(crate) fn write_plain_buffer(self, cursor: &mut Cursor<Vec<u8>>) -> u8 {
        let Self { name, value, checksum } = self;

        let mut cell_checksum = 0u8;
        cell_checksum = crc_bytes(cell_checksum, name.as_bytes());
        cell_checksum = value.crc8_checksum(cell_checksum);

        cursor.write_u8(TAG_CELL_NAME).unwrap();
        cursor.write_u32::<LittleEndian>(name.len() as u32).unwrap();
        cursor.write_all(name.as_bytes()).unwrap();
        cursor.write_u8(TAG_CELL_VALUE).unwrap();
        value.write_plain_buffer(cursor);
        cursor.write_u8(TAG_CELL_CHECKSUM).unwrap();
        cursor.write_u8(cell_checksum).unwrap();

        cell_checksum
    }
}

#[cfg(test)]
mod test_primary_key {
    use crate::model::{PrimaryKey, PrimaryKeyColumn, PrimaryKeyValue};

    #[test]
    fn test_build_primary_key() {
        let bytes_from_java_sdk = [
            0x75u8, 0x00, 0x00, 0x00, 0x01, 0x03, 0x04, 0x07, 0x00, 0x00, 0x00, 0x75, 0x73, 0x65, 0x72, 0x5F, 0x69, 0x64, 0x05, 0x29, 0x00, 0x00, 0x00, 0x03,
            0x24, 0x00, 0x00, 0x00, 0x30, 0x30, 0x30, 0x35, 0x33, 0x35, 0x38, 0x41, 0x2D, 0x44, 0x43, 0x41, 0x46, 0x2D, 0x36, 0x36, 0x35, 0x45, 0x2D, 0x45,
            0x45, 0x43, 0x46, 0x2D, 0x44, 0x39, 0x39, 0x33, 0x35, 0x45, 0x38, 0x32, 0x31, 0x42, 0x38, 0x37, 0x0A, 0xC8, 0x09, 0x45,
        ];

        let pk = PrimaryKey {
            keys: vec![PrimaryKeyColumn {
                name: "user_id".to_string(),
                value: PrimaryKeyValue::String("0005358A-DCAF-665E-EECF-D9935E821B87".to_string()),
                checksum: None,
            }],
        };

        let size = pk.compute_size_with_header();
        assert_eq!(68, size);

        let buf = pk.into_plain_buffer(true);
        assert_eq!(bytes_from_java_sdk, &buf[..]);
        println!("{:?}", buf);
    }
}
