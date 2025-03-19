use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::{
    OtsResult,
    crc8::crc_u8,
    error::OtsError,
    protos::plain_buffer::{self, HEADER, LITTLE_ENDIAN_32_SIZE, MASK_HEADER, TAG_ROW_CHECKSUM, TAG_ROW_DATA, TAG_ROW_PK},
};

use super::{Column, ColumnValue, PrimaryKeyColumn, PrimaryKeyValue};

/// 宽表模型的行
#[derive(Debug, Clone, Default)]
pub struct Row {
    /// 主键列
    pub primary_keys: Vec<PrimaryKeyColumn>,

    /// 数据列
    pub columns: Vec<Column>,
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
    pub fn get_column_value(&self, name: &str) -> Option<&ColumnValue> {
        self.columns.iter().find(|c| c.name.as_str() == name).map(|c| &c.value)
    }

    /// 计算一个行的 plain buffer
    pub(crate) fn compute_size(&self, masks: u32) -> u32 {
        let size = if masks & MASK_HEADER == MASK_HEADER { LITTLE_ENDIAN_32_SIZE } else { 0u32 };

        size + self.primary_keys.iter().map(|k| k.compute_size()).sum::<u32>() + self.columns.iter().map(|c| c.compute_size()).sum::<u32>()
    }

    /// 输出 plain buffer 的编码
    pub(crate) fn encode_plain_buffer(&self, masks: u32) -> Vec<u8> {
        let size = self.compute_size(masks);

        let mut cursor = Cursor::new(vec![0u8; size as usize]);

        if masks & MASK_HEADER == MASK_HEADER {
            cursor.write_u32::<LittleEndian>(HEADER).unwrap();
        }

        self.write_plain_buffer(&mut cursor, 0u32);

        cursor.into_inner()
    }

    pub(crate) fn write_plain_buffer(&self, cursor: &mut Cursor<Vec<u8>>, _masks: u32) {
        let Self { primary_keys, columns } = self;

        cursor.write_u8(TAG_ROW_PK).unwrap();
        for key_col in primary_keys {
            key_col.write_plain_buffer(cursor);
        }

        if !columns.is_empty() {
            cursor.write_u8(TAG_ROW_DATA).unwrap();

            for col in columns {
                col.write_plain_buffer(cursor);
            }
        }

        cursor.write_u8(TAG_ROW_CHECKSUM).unwrap();
        cursor.write_u8(self.crc8_checksum()).unwrap();
    }

    /// 从 cursor 构建行
    pub(crate) fn read_plain_buffer(cursor: &mut Cursor<Vec<u8>>) -> OtsResult<Self> {
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
                        let pkc = PrimaryKeyColumn::read_plain_buffer(cursor)?;
                        // log::debug!("primary key column read: {:#?}", pkc);
                        primary_keys.push(pkc);
                    }

                    RowType::Column => {
                        let cell = Column::read_plain_buffer(cursor)?;
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
                            "data data checksum validation failed. calculated: {}, received: {}",
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

    /// 计算整行的校验码
    pub(crate) fn crc8_checksum(&self) -> u8 {
        let mut checksum = 0u8;
        for key_col in &self.primary_keys {
            let cell_checksum = key_col.crc8_checksum();
            // log::debug!("primary key: {} crc8 checksum: {:02x}", key_col.name, cell_checksum);
            checksum = crc_u8(checksum, cell_checksum);
        }

        for col in &self.columns {
            let cell_checksum = col.crc8_checksum();
            // log::debug!("column key: {} crc8 checksum: {:02x}", col.name, cell_checksum);
            checksum = crc_u8(checksum, cell_checksum);
        }

        //TODO: set byte according to delete flag
        checksum = crc_u8(checksum, 0u8);
        checksum
    }
}

#[cfg(test)]
mod test_row {
    use base64::{Engine, prelude::BASE64_STANDARD};
    use prost::Message;

    use crate::{
        OtsClient, OtsOp, OtsRequest,
        model::{Column, ColumnValue, PrimaryKey, PrimaryKeyColumn},
        protos::{
            plain_buffer::{MASK_HEADER, MASK_ROW_CHECKSUM},
            table_store::{GetRowRequest, GetRowResponse},
        },
    };

    use std::sync::Once;

    use super::Row;

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::dotenv().unwrap();
        });
    }

    // #[tokio::test]
    // async fn get_row_no_col() {
    //     setup();
    //     let client = OtsClient::from_env();

    //     let pk = PrimaryKey {
    //         keys: vec![
    //             PrimaryKeyColumn::with_string_value("school_id", "1"),
    //             PrimaryKeyColumn::with_integer_value("id", 1742373697699000)
    //         ]
    //     };

    //     let pk_bytes = pk.encode_plain_buffer(MASK_HEADER | MASK_ROW_CHECKSUM);

    //     let msg = GetRowRequest {
    //         table_name: "schools".to_string(),
    //         primary_key: pk_bytes,
    //         columns_to_get: vec![],
    //         time_range: None,
    //         max_versions: Some(1),
    //         filter: None,
    //         start_column: None,
    //         end_column: None,
    //         token: None,
    //         transaction_id: None,
    //     };

    //     let body_bytes = msg.encode_to_vec();

    //     let req = OtsRequest {
    //         method: reqwest::Method::POST,
    //         operation: OtsOp::GetRow,
    //         body: body_bytes,
    //         ..Default::default()
    //     };

    //     let response = client.send(req).await.unwrap();
    //     let response_bytes = response.bytes().await.unwrap();
    //     let msg = GetRowResponse::decode(response_bytes).unwrap();
    //     let row_bytes = &msg.row;

    //     std::fs::write("/home/yuanyq/Downloads/aliyun-plainbuffer/row-no-col.data", &row_bytes).unwrap();
    // }

    // #[tokio::test]
    // async fn get_row_1_col() {
    //     setup();
    //     let client = OtsClient::from_env();

    //     let pk = PrimaryKey {
    //         keys: vec![
    //             PrimaryKeyColumn::with_string_value("school_id", "2"),
    //             PrimaryKeyColumn::with_integer_value("id", 1742378007415000)
    //         ]
    //     };

    //     let pk_bytes = pk.encode_plain_buffer(MASK_HEADER | MASK_ROW_CHECKSUM);

    //     let msg = GetRowRequest {
    //         table_name: "schools".to_string(),
    //         primary_key: pk_bytes,
    //         columns_to_get: vec![],
    //         time_range: None,
    //         max_versions: Some(1),
    //         filter: None,
    //         start_column: None,
    //         end_column: None,
    //         token: None,
    //         transaction_id: None,
    //     };

    //     let body_bytes = msg.encode_to_vec();

    //     let req = OtsRequest {
    //         method: reqwest::Method::POST,
    //         operation: OtsOp::GetRow,
    //         body: body_bytes,
    //         ..Default::default()
    //     };

    //     let response = client.send(req).await.unwrap();
    //     let response_bytes = response.bytes().await.unwrap();
    //     let msg = GetRowResponse::decode(response_bytes).unwrap();
    //     let row_bytes = &msg.row;

    //     std::fs::write("/home/yuanyq/Downloads/aliyun-plainbuffer/row-1-col.data", &row_bytes).unwrap();
    // }

    /// 测试主键不包含数据列的 plain buffer 编码
    #[test]
    fn test_row_no_col() {
        let md5_expected = "gpADtIzJpJRgXgSMKOUHTQ==";

        let row = Row {
            primary_keys: vec![
                PrimaryKeyColumn::with_string_value("school_id", "1"),
                PrimaryKeyColumn::with_integer_value("id", 1742373697699000),
            ],
            columns: vec![],
        };

        let pb_bytes = row.encode_plain_buffer(MASK_HEADER | MASK_ROW_CHECKSUM);
        let md5_calc = BASE64_STANDARD.encode(md5::compute(&pb_bytes).to_vec());

        assert_eq!(md5_expected, md5_calc);
    }

    #[test]
    fn test_row_1_col() {
        setup();

        let md5_expected = "LkUq5OPGrWhSyrC7qenr2A==";

        let row = Row {
            primary_keys: vec![
                PrimaryKeyColumn::with_string_value("school_id", "2"),
                PrimaryKeyColumn::with_integer_value("id", 1742378007415000),
            ],
            columns: vec![Column {
                name: "name".to_string(),
                value: ColumnValue::String("School-A".to_string()),
                timestamp: Some(1742378007415),
            }],
        };

        log::debug!("row CRC8 checksum = {:02x}", row.crc8_checksum());

        let pb_bytes = row.encode_plain_buffer(MASK_HEADER | MASK_ROW_CHECKSUM);
        std::fs::write("/home/yuanyq/Downloads/aliyun-plainbuffer/row-1-col-mine.data", &pb_bytes).unwrap();
        let md5_calc = BASE64_STANDARD.encode(md5::compute(&pb_bytes).to_vec());

        assert_eq!(md5_expected, md5_calc);
    }
}
