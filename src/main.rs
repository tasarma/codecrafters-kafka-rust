#![allow(unused_imports)]
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    io::{Cursor, Read, Write},
    net::TcpListener,
};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let mut buffer = [0u8; 1024];
                // How many actual bytes we got
                let bytes_read = stream.read(&mut buffer).unwrap();

                let mut cursor = Cursor::new(&buffer[..bytes_read]);

                let _message_size = cursor.read_u32::<BigEndian>().unwrap();
                let _request_api_key = cursor.read_u16::<BigEndian>().unwrap();
                let _request_api_version = cursor.read_u16::<BigEndian>().unwrap();
                let correlation_id = cursor.read_u32::<BigEndian>().unwrap();

                let mut response_body = Vec::new();

                response_body
                    .write_u32::<BigEndian>(correlation_id)
                    .unwrap();

                response_body
                    .write_u16::<BigEndian>(0) // 0 means No error
                    .unwrap();

                // Support 1 API key
                response_body.write_u8(1).unwrap();

                // API Key: 18 with min and max supported api versions
                response_body.write_u16::<BigEndian>(18).unwrap();
                response_body.write_u16::<BigEndian>(0).unwrap();
                response_body.write_u16::<BigEndian>(4).unwrap();

                // ThrottleTimeMs = 0 for v4
                response_body.write_u32::<BigEndian>(0).unwrap();

                let mut response = Vec::new();
                let response_message_size = response_body.len() as u32;

                response
                    .write_u32::<BigEndian>(response_message_size)
                    .unwrap();
                response.extend_from_slice(&response_body);

                stream.write_all(&response).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
