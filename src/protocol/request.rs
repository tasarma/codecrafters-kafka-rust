use byteorder::{BigEndian, ReadBytesExt};
use std::{error::Error, io::Cursor};

#[derive(Debug)]
pub struct KafkaRequest {
    pub message_size: u32,
    pub api_key: u16,
    pub api_version: u16,
    pub correlation_id: u32,
}

impl KafkaRequest {
    pub fn parse(buffer: &[u8]) -> Result<Self, Box<dyn Error>> {
        let mut cursor = Cursor::new(buffer);

        Ok(KafkaRequest {
            message_size: cursor.read_u32::<BigEndian>()?,
            api_key: cursor.read_u16::<BigEndian>()?,
            api_version: cursor.read_u16::<BigEndian>()?,
            correlation_id: cursor.read_u32::<BigEndian>()?,
        })
    }
}
