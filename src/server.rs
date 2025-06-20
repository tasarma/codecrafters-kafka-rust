use crate::protocol::{request::KafkaRequest, response::ApiVersionResponse};
use std::{
    error::Error,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

const BIND_ADDRESS: &str = "127.0.0.1:9092";
const BUFFER_SIZE: usize = 1024;

fn handle_client(mut stream: TcpStream) -> Result<(), Box<dyn Error>> {
    let mut buffer = [0u8; BUFFER_SIZE];

    loop {
        let bytes_read = stream.read(&mut buffer)?;
        println!("Received {} bytes from {}", bytes_read, stream.peer_addr()?);
        if bytes_read == 0 {
            println!("Client disconnected without sending data");
            break;
        }

        let request = KafkaRequest::parse(&buffer[..bytes_read])?;
        println!("Received request: {:?}", request);

        // Handle API version request (API key 18)
        if request.api_key == 18 {
            let response = ApiVersionResponse::new(request.correlation_id, request.api_version);
            let response_bytes = response.serialize(request.api_version)?;

            stream.write_all(&response_bytes)?;

            println!(
                "Sent API version response for correlation ID: {} (version: {})",
                request.correlation_id, request.api_version
            );
        } else {
            println!("Unsupported API key: {}", request.api_key);
            // Could send an error response here for unsupported API keys
        }
    }

    Ok(())
}

pub fn start_server() -> Result<(), Box<dyn Error>> {
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
