mod protocol;
mod server;

use server::start_server;

fn main() {
    println!("Starting Kafka protocol server...");

    if let Err(e) = start_server() {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    }
}
