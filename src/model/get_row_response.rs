use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt};
use prost::Message;

use crate::{
    OtsResult,
    error::OtsError,
    protos::{plain_buffer::HEADER, table_store::ConsumedCapacity},
};

use super::Row;

#[derive(Clone, Default, Debug)]
pub struct GetRowResponse {
    pub consumed: ConsumedCapacity,
    pub row: Option<Row>,
    pub next_token: Option<Vec<u8>>,
}

impl GetRowResponse {
    pub fn decode(bytes: Vec<u8>) -> OtsResult<Self> {
        let msg = crate::protos::table_store::GetRowResponse::decode(bytes.as_slice())?;
        let crate::protos::table_store::GetRowResponse {
            consumed,
            row: row_bytes,
            next_token,
        } = msg;

        let row = if !row_bytes.is_empty() {
            let mut cursor = Cursor::new(row_bytes);
            let header = cursor.read_u32::<LittleEndian>()?;

            if header != HEADER {
                return Err(OtsError::PlainBufferError(format!("invalid message header: {}", header)));
            }

            Some(Row::from_cursor(&mut cursor)?)
        } else {
            None
        };

        Ok(Self { consumed, row, next_token })
    }
}
