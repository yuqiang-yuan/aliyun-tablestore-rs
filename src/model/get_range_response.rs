use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt};
use prost::Message;

use crate::{
    OtsResult,
    error::OtsError,
    protos::{plain_buffer::HEADER, table_store::ConsumedCapacity},
};

use super::{PrimaryKeyColumn, Row};

#[derive(Clone, Default, Debug)]
pub struct GetRangeResponse {
    pub consumed: ConsumedCapacity,
    pub rows: Vec<Row>,
    pub next_token: Option<Vec<u8>>,

    /// 本次操作的断点信息
    ///
    /// - 当返回值为空时，表示本次 `GetRange` 的响应消息中已包含请求范围内的所有数据。
    /// - 当返回值不为空时，表示本次 `GetRange` 的响应消息中只包含了 `[inclusive_start_primary_key, next_start_primary_key)` 间的数据。
    ///   如果需要继续读取剩下的数据，则需要将 `next_start_primary_key` 作为 `inclusive_start_primary_key`，原始请求中的 `exclusive_end_primary_key` 作为 `exclusive_end_primary_key` 继续执行 `GetRange` 操作。
    pub next_start_primary_key: Option<Vec<PrimaryKeyColumn>>,
}

impl GetRangeResponse {
    pub fn decode(bytes: Vec<u8>) -> OtsResult<Self> {
        let msg = crate::protos::table_store::GetRangeResponse::decode(bytes.as_slice())?;
        let crate::protos::table_store::GetRangeResponse {
            consumed,
            rows: rows_bytes,
            next_start_primary_key,
            next_token,
        } = msg;

        let next_pk = if let Some(bytes) = next_start_primary_key {
            std::fs::write("/home/yuanyq/Downloads/aliyun-plainbuffer/next-pk.data", &bytes).unwrap();

            let mut cursor = Cursor::new(bytes);
            let header = cursor.read_u32::<LittleEndian>()?;

            if header != HEADER {
                return Err(OtsError::PlainBufferError(format!("invalid message header: {}", header)));
            }

            let row = Row::from_cursor(&mut cursor)?;

            Some(row.primary_keys)
        } else {
            None
        };

        let mut rows = vec![];

        // std::fs::write("/home/yuanyq/Downloads/aliyun-plainbuffer/get-range-1.data", &rows_bytes).unwrap();

        if !rows_bytes.is_empty() {
            let mut cursor = Cursor::new(rows_bytes);
            let header = cursor.read_u32::<LittleEndian>()?;

            if header != HEADER {
                return Err(OtsError::PlainBufferError(format!("invalid message header: {}", header)));
            }

            loop {
                if cursor.position() as usize >= cursor.get_ref().len() - 1 {
                    break;
                }

                let row = Row::from_cursor(&mut cursor)?;
                rows.push(row);
            }
        }

        Ok(Self {
            consumed,
            rows,
            next_token,
            next_start_primary_key: next_pk,
        })
    }
}
