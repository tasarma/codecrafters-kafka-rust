use byteorder::{BigEndian, WriteBytesExt};
use std::error::Error;

#[derive(Debug)]
pub struct ApiVersionResponse {
    pub correlation_id: u32,
    pub error_code: u16,
    pub api_keys: Vec<ApiKeyInfo>,
    pub throttle_time_ms: u32,
}

#[derive(Debug)]
pub struct ApiKeyInfo {
    pub api_key: u16,
    pub min_version: u16,
    pub max_version: u16,
}

pub enum ErrorCode {
    NoError = 0,
    UnsupportedError = 35,
}

impl ApiVersionResponse {
    pub fn new(correlation_id: u32, api_version: u16) -> Self {
        let error_code = if (0..=4).contains(&api_version) {
            ErrorCode::NoError
        } else {
            ErrorCode::UnsupportedError
        } as u16;

        ApiVersionResponse {
            correlation_id,
            error_code,
            api_keys: vec![
                ApiKeyInfo {
                    api_key: 18,
                    min_version: 0,
                    max_version: 4,
                },
                ApiKeyInfo {
                    api_key: 75,
                    min_version: 0,
                    max_version: 0,
                },
            ],
            throttle_time_ms: 0,
        }
    }

    pub fn serialize(&self, api_version: u16) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut body = Vec::new();

        // Write correlation ID
        body.write_u32::<BigEndian>(self.correlation_id)?;

        // Write error code
        body.write_u16::<BigEndian>(self.error_code)?;

        if api_version >= 3 {
            // For v3+, use compact array format (length + 1)
            body.write_u8((self.api_keys.len() + 1) as u8)?;
        } else {
            // For v0-v2, use regular array format
            body.write_u32::<BigEndian>(self.api_keys.len() as u32)?;
        }

        // Write each API key info
        for api_key_info in &self.api_keys {
            body.write_u16::<BigEndian>(api_key_info.api_key)?;
            body.write_u16::<BigEndian>(api_key_info.min_version)?;
            body.write_u16::<BigEndian>(api_key_info.max_version)?;

            if api_version >= 3 {
                // Tagged fields (empty for now)
                body.write_u8(0)?;
            }
        }

        if api_version >= 1 {
            // Write throttle time (for v1+)
            body.write_u32::<BigEndian>(self.throttle_time_ms)?;
        }

        if api_version >= 3 {
            // Tagged fields at the end (empty for now)
            body.write_u8(0)?;
        }

        // Prepend message size
        let mut response = Vec::new();
        response.write_u32::<BigEndian>(body.len() as u32)?;
        response.extend_from_slice(&body);

        Ok(response)
    }
}
