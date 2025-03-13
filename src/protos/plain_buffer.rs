//! Aliyun plain buffer. See [https://help.aliyun.com/zh/tablestore/developer-reference/plainbuffer] for more details.

use std::io::{Cursor, Write};

use byteorder::{LittleEndian, WriteBytesExt};

use crate::{
    crc8::{crc_bytes, crc_u8},
    model::primary_key::{PrimaryKey, PrimaryKeyColumn, PrimaryKeyValue},
};

pub const LITTLE_ENDIAN_32_SIZE: u32 = 4;
pub const LITTLE_ENDIAN_64_SIZE: u32 = 8;

pub const HEADER: u32 = 0x75;

// tag types
pub const TAG_ROW_PK: u8 = 0x1;
pub const TAG_ROW_DATA: u8 = 0x2;
pub const TAG_CELL: u8 = 0x3;
pub const TAG_CELL_NAME: u8 = 0x4;
pub const TAG_CELL_VALUE: u8 = 0x5;
pub const TAG_CELL_TYPE: u8 = 0x6;
pub const TAG_CELL_TIMESTAMP: u8 = 0x7;
pub const TAG_DELETE_ROW_MARKER: u8 = 0x8;
pub const TAG_ROW_CHECKSUM: u8 = 0x9;
pub const TAG_CELL_CHECKSUM: u8 = 0x0A;
pub const TAG_EXTENSION: u8 = 0x0B;
pub const TAG_SEQ_INFO: u8 = 0x0C;
pub const TAG_SEQ_INFO_EPOCH: u8 = 0x0D;
pub const TAG_SEQ_INFO_TS: u8 = 0x0E;
pub const TAG_SEQ_INFO_ROW_INDEX: u8 = 0x0F;

// cell operation types
pub const DELETE_ALL_VERSION: u8 = 0x1;
pub const DELETE_ONE_VERSION: u8 = 0x3;
pub const INCREMENT: u8 = 0x4;

// variant types
pub const VT_INTEGER: u8 = 0x0;
pub const VT_DOUBLE: u8 = 0x1;
pub const VT_BOOLEAN: u8 = 0x2;
pub const VT_STRING: u8 = 0x3;
pub const VT_NULL: u8 = 0x6;
pub const VT_BLOB: u8 = 0x7;
pub const VT_INF_MIN: u8 = 0x9;
pub const VT_INF_MAX: u8 = 0xa;
pub const VT_AUTO_INCREMENT: u8 = 0xb;

#[derive(Default)]
pub struct PlainBufferCodedStream {
    buffer: Cursor<Vec<u8>>,
}

impl PlainBufferCodedStream {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Cursor::new(Vec::with_capacity(capacity)),
        }
    }

    pub fn new() -> Self {
        Self {
            buffer: Cursor::new(Vec::new()),
        }
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.buffer.into_inner()
    }

    pub fn write_u8(&mut self, b: u8) -> &mut Self {
        self.buffer.write_all(&[b]).unwrap();
        self
    }

    pub fn write_bytes(&mut self, buf: &[u8]) -> &mut Self {
        self.buffer.write_all(buf).unwrap();
        self
    }

    pub fn write_i32_le(&mut self, n: i32) -> &mut Self {
        self.buffer.write_i32::<LittleEndian>(n).unwrap();
        self
    }

    pub fn write_u32_le(&mut self, n: u32) -> &mut Self {
        self.buffer.write_u32::<LittleEndian>(n).unwrap();
        self
    }

    pub fn write_i64_le(&mut self, n: i64) -> &mut Self {
        self.buffer.write_i64::<LittleEndian>(n).unwrap();
        self
    }

    pub fn write_u64_le(&mut self, n: u64) -> &mut Self {
        self.buffer.write_u64::<LittleEndian>(n).unwrap();
        self
    }

    pub fn write_f64_le(&mut self, v: f64) -> &mut Self {
        self.buffer.write_f64::<LittleEndian>(v).unwrap();
        self
    }

    pub fn write_f32_le(&mut self, v: f32) -> &mut Self {
        self.buffer.write_f32::<LittleEndian>(v).unwrap();
        self
    }

    pub fn write_bool(&mut self, b: bool) -> &mut Self {
        self.write_u8(if b { 0x01 } else { 0x00 });
        self
    }

    pub fn write_primary_key_column(&mut self, kc: &PrimaryKeyColumn, cell_checksum: u8) -> u8 {
        self.write_u8(TAG_CELL)
            .write_u8(TAG_CELL_NAME)
            .write_u32_le(kc.name.len() as u32)
            .write_bytes(kc.name.as_bytes())
            .write_u8(TAG_CELL_VALUE)
            .write_primary_key_value(&kc.value)
            .write_u8(TAG_CELL_CHECKSUM)
            .write_u8(cell_checksum);
        0u8
    }

    pub fn write_primary_key_value(&mut self, kv: &PrimaryKeyValue) -> &mut Self {
        match kv {
            PrimaryKeyValue::Integer(n) => self.write_u32_le(LITTLE_ENDIAN_64_SIZE + 1).write_u8(VT_INTEGER).write_i64_le(*n),

            PrimaryKeyValue::String(s) => self
                .write_u32_le(1 + LITTLE_ENDIAN_32_SIZE + s.len() as u32)
                .write_u8(VT_STRING)
                .write_u32_le(s.len() as u32)
                .write_bytes(s.as_bytes()),

            PrimaryKeyValue::Binary(buf) => self
                .write_u32_le(1 + LITTLE_ENDIAN_32_SIZE + buf.len() as u32)
                .write_u8(VT_BLOB)
                .write_u32_le(buf.len() as u32)
                .write_bytes(buf),

            PrimaryKeyValue::InfMin => self.write_u32_le(1).write_u8(VT_INF_MIN),

            PrimaryKeyValue::InfMax => self.write_u32_le(1).write_u8(VT_INF_MAX),

            PrimaryKeyValue::AutoIncrement => self.write_u32_le(1).write_u8(VT_AUTO_INCREMENT),
        }
    }

    pub fn write_cell_name(&mut self, name: &str, cell_checksum: u8) -> u8 {
        self.write_u8(TAG_CELL_NAME).write_u32_le(name.len() as u32).write_bytes(name.as_bytes());

        crc_bytes(cell_checksum, name.as_bytes())
    }

    pub fn build_primary_key_with_header(pk: &PrimaryKey) -> Vec<u8> {
        let mut coded_stream = Self::with_capacity(pk.compute_size_with_header() as usize);
        coded_stream.write_u32_le(HEADER).write_u8(TAG_ROW_PK);

        let mut row_checksum = 0u8;

        for key_col in &pk.keys {
            let mut cell_checksum = 0u8;
            cell_checksum = crc_bytes(cell_checksum, key_col.name.as_bytes());
            cell_checksum = key_col.value.crc8_checksum(cell_checksum);
            coded_stream.write_primary_key_column(key_col, cell_checksum);
            row_checksum = crc_u8(row_checksum, cell_checksum);
        }

        row_checksum = crc_u8(row_checksum, 0u8);
        coded_stream.write_u8(TAG_ROW_CHECKSUM).write_u8(row_checksum);

        coded_stream.into_inner()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        model::primary_key::{PrimaryKey, PrimaryKeyColumn, PrimaryKeyValue},
        protos::plain_buffer::PlainBufferCodedStream,
    };

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
                auto_increment: false,
            }],
        };

        let size = pk.compute_size_with_header();
        assert_eq!(68, size);

        let buf = PlainBufferCodedStream::build_primary_key_with_header(&pk);
        assert_eq!(bytes_from_java_sdk, &buf[..]);
        println!("{:?}", buf);
    }
}
