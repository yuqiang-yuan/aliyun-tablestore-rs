use crate::{
    crc8::{crc_bytes, crc_i64, crc_u8, crc_u32},
    protos::plain_buffer::{self, LITTLE_ENDIAN_64_SIZE, VT_AUTO_INCREMENT, VT_BLOB, VT_INF_MAX, VT_INF_MIN, VT_INTEGER, VT_STRING},
};

#[derive(Debug, Clone)]
pub struct PrimaryKey {
    pub keys: Vec<PrimaryKeyColumn>,
}

impl PrimaryKey {
    pub(crate) fn compute_size(&self) -> u32 {
        1u32 + self.keys.iter().map(|k| k.compute_size()).sum::<u32>()
    }

    pub(crate) fn compute_size_with_header(&self) -> u32 {
        plain_buffer::LITTLE_ENDIAN_32_SIZE + self.compute_size() + 2
    }
}

#[derive(Debug, Clone)]
pub enum PrimaryKeyValue {
    Integer(i64),
    String(String),
    Binary(Vec<u8>),
    InfMax,
    InfMin,
    AutoIncrement,
}

impl PrimaryKeyValue {
    pub(crate) fn compute_size(&self) -> u32 {
        let size = plain_buffer::LITTLE_ENDIAN_32_SIZE + 1;

        match self {
            Self::Integer(_) => size + LITTLE_ENDIAN_64_SIZE,
            Self::String(s) => size + plain_buffer::LITTLE_ENDIAN_32_SIZE + s.len() as u32,
            Self::Binary(buf) => size + plain_buffer::LITTLE_ENDIAN_32_SIZE + buf.len() as u32,
            Self::InfMax | Self::InfMin | Self::AutoIncrement => size,
        }
    }

    pub(crate) fn crc8_checksum(&self, input_crc: u8) -> u8 {
        let mut crc = input_crc;

        match self {
            Self::InfMin => crc_u8(crc, VT_INF_MIN),
            Self::InfMax => crc_u8(crc, VT_INF_MAX),
            Self::AutoIncrement => crc_u8(crc, VT_AUTO_INCREMENT),

            Self::Integer(n) => {
                crc = crc_u8(crc, VT_INTEGER);
                crc_i64(crc, *n)
            }

            Self::String(s) => {
                crc = crc_u8(crc, VT_STRING);
                crc = crc_u32(crc, s.len() as u32);
                crc_bytes(crc, s.as_bytes())
            }

            Self::Binary(buf) => {
                crc = crc_u8(crc, VT_BLOB);
                crc = crc_u32(crc, buf.len() as u32);
                crc_bytes(crc, buf)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct PrimaryKeyColumn {
    pub name: String,
    pub value: PrimaryKeyValue,
    pub auto_increment: bool,
}

impl PrimaryKeyColumn {
    pub fn compute_size(&self) -> u32 {
        2u32 + plain_buffer::LITTLE_ENDIAN_32_SIZE + self.name.len() as u32 + 1 + self.value.compute_size() + 2
    }
}
