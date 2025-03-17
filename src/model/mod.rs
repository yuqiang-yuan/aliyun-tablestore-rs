mod defined_column;
mod primary_key;
mod row;

pub use defined_column::*;
pub use primary_key::*;
pub use row::*;

#[cfg(test)]
mod test_model {
    use byteorder::{LittleEndian, ReadBytesExt};

    use crate::protos::plain_buffer;

    use super::Row;

    use std::{io::Cursor, sync::Once};

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::dotenv().unwrap();
        });
    }

    #[test]
    fn test_decode_plain_buffer() {
        setup();
        let bytes = std::fs::read("/home/yuanyq/Downloads/aliyun-plainbuffer/get-row-response-versions.data").unwrap();
        let mut cursor = Cursor::new(bytes);
        let header = cursor.read_u32::<LittleEndian>().unwrap();

        assert_eq!(plain_buffer::HEADER, header);

        let row = Row::from_cursor(&mut cursor).unwrap();
        log::debug!("{:#?}", row);
    }
}
