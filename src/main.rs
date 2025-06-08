#![allow(unused_imports)]
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    error::Error,
    io::{Cursor, Read, Write},
    net::{TcpListener, TcpStream},
};

const BIND_ADDRESS: &str = "127.0.0.1:9092";
const BUFFER_SIZE: usize = 1024;

#[derive(Debug)]
struct KafkaRequest {
    message_size: u32,
    api_key: u16,
    api_version: u16,
    correlation_id: u32,
}

#[derive(Debug)]
struct ApiVersionResponse {
    correlation_id: u32,
    error_code: u16,
    api_keys: Vec<ApiKeyInfo>,
    throttle_time_ms: u32,
}

#[derive(Debug)]
struct ApiKeyInfo {
    api_key: u16,
    min_version: u16,
    max_version: u16,
}

enum ErrorCode {
    NoError = 0,
    UnsupportedError = 35,
}

impl KafkaRequest {
    fn parse(buffer: &[u8]) -> Result<Self, Box<dyn Error>> {
        let mut cursor = Cursor::new(buffer);

        Ok(KafkaRequest {
            message_size: cursor.read_u32::<BigEndian>()?,
            api_key: cursor.read_u16::<BigEndian>()?,
            api_version: cursor.read_u16::<BigEndian>()?,
            correlation_id: cursor.read_u32::<BigEndian>()?,
        })
    }
}

impl ApiVersionResponse {
    fn new(correlation_id: u32, api_version: u16) -> Self {
        let error_code = if (0..=4).contains(&api_version) {
            ErrorCode::NoError
        } else {
            ErrorCode::UnsupportedError
        } as u16;

        ApiVersionResponse {
            correlation_id,
            error_code,
            api_keys: vec![ApiKeyInfo {
                api_key: 18,
                min_version: 0,
                max_version: 4,
            }],
            throttle_time_ms: 0,
        }
    }

    fn serialize(&self, api_version: u16) -> Result<Vec<u8>, Box<dyn Error>> {
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

fn handle_client(mut stream: TcpStream) -> Result<(), Box<dyn Error>> {
    println!("Accepted new connection from: {:?}", stream.peer_addr()?);

    let mut buffer = [0u8; BUFFER_SIZE];
    let bytes_read = stream.read(&mut buffer)?;

    if bytes_read == 0 {
        println!("Client disconnected without sending data");
        return Ok(());
    }

    let request = KafkaRequest::parse(&buffer[..bytes_read])?;
    println!("Received request: {:?}", request);

    // Handle API version request (API key 18)
    if request.api_key == 18 {
        let response = ApiVersionResponse::new(request.correlation_id, request.api_version);
        let response_bytes = response.serialize(request.api_version)?;

        stream.write_all(&response_bytes)?;
        stream.flush()?;

        println!(
            "Sent API version response for correlation ID: {} (version: {})",
            request.correlation_id, request.api_version
        );
    } else {
        println!("Unsupported API key: {}", request.api_key);
        // Could send an error response here for unsupported API keys
    }

    Ok(())
}

fn start_server() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind(BIND_ADDRESS)?;
    println!("Kafka protocol server listening on {}", BIND_ADDRESS);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                if let Err(e) = handle_client(stream) {
                    eprintln!("Error handling client: {}", e);
                }
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
            }
        }
    }

    Ok(())
}

fn main() {
    println!("Starting Kafka protocol server...");

    if let Err(e) = start_server() {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    }
}
