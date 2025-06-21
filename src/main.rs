mod protocol;
mod server;

use server::start_server;

#[tokio::main]
async fn main() {
    println!("\n\nStarting Kafka protocol server...");

    if let Err(e) = start_server().await {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    }
}
