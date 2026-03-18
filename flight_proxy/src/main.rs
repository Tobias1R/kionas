use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    env_logger::init();
    log::info!("flight_proxy bootstrap initialized");
    log::info!("flight_proxy service scaffolding is ready; endpoint implementation is pending");
    Ok(())
}
