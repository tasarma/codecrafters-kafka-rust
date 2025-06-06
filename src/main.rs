#![allow(unused_imports)]
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    io::{Cursor, Read, Write},
    net::TcpListener,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let mut buffer = [0u8; 1024];
                // How many actual bytes we got
                let bytes_read = stream.read(&mut buffer).unwrap();
                println!("Received {} bytes", bytes_read);

                let mut cursor = Cursor::new(&buffer[..bytes_read]);

                let _message_size = cursor.read_u32::<BigEndian>().unwrap();
                let _api_key = cursor.read_u16::<BigEndian>().unwrap();
                let _request_api_version = cursor.read_u16::<BigEndian>().unwrap();
                let correlation_id = cursor.read_u32::<BigEndian>().unwrap();

                let mut response = Vec::new();
                response.write_u32::<BigEndian>(0).unwrap();
                response.write_u32::<BigEndian>(correlation_id).unwrap();
                println!("{:?}", response);

                stream.write_all(&response).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
