use crate::protocol::{request::KafkaRequest, response::ApiVersionResponse};
use std::error::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const BIND_ADDRESS: &str = "127.0.0.1:9092";
const BUFFER_SIZE: usize = 1024;

async fn handle_client(mut socket: TcpStream) -> Result<(), Box<dyn Error>> {
    let mut buffer = [0u8; BUFFER_SIZE];

    loop {
        match socket.read(&mut buffer).await {
            // Return value of `Ok(0)` signifies that the remote has closed
            Ok(0) => return Ok(()),
            Ok(bytes_read) => {
                println!("Received {} bytes from {}", bytes_read, socket.peer_addr()?);
                let request = KafkaRequest::parse(&buffer[..bytes_read])?;
                println!("Received request: {:?}", request);
                // Handle API version request (API key 18)
                //if request.api_key == 18 {
                let response = ApiVersionResponse::new(request.correlation_id, request.api_version);
                let response_bytes = response.serialize(request.api_version)?;

                socket.write_all(&response_bytes).await?;

                println!(
                    "Sent API version response for correlation ID: {} (version: {})",
                    request.correlation_id, request.api_version
                );
                //} else {
                //    println!("Unsupported API key: {}", request.api_key);
                // Could send an error response here for unsupported API keys
                //}
            }
            Err(_) => return Ok(()),
        }
    }
}

pub async fn start_server() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind(BIND_ADDRESS).await?;
    println!("Kafka protocol server listening on {}", BIND_ADDRESS);

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = handle_client(socket).await {
                eprintln!("Error handling client: {}", e);
            }
        });
    }
}
