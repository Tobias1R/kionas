use kionas::utils::{print_memory_usage, print_server_info};

use std::sync::Arc;

use crate::{
    janitor, tls as tlsmod,
    warehouse::state::{SharedData, SharedState},
};
use kionas::config::AppConfig;

use crate::auth_setup;
use crate::handlers;
use tokio::sync::Mutex;

use crate::services::interops_service_server::InteropsService;
use crate::services::warehouse_service_server::WarehouseService;

use kionas::utils::resolve_hostname;
use std::error::Error;

pub async fn run(config: AppConfig) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Initialize logging
    if let Err(e) = kionas::logging::init_logging(
        &config.logging.level,
        &config.logging.output,
        &config.logging.format,
    ) {
        eprintln!("Failed to initialize logging: {}", e);
    } else {
        println!(
            "Logging initialized with level: {}, output: {}, format: {}",
            config.logging.level, config.logging.output, config.logging.format
        );
    }

    // Initialize cluster config in Consul
    // let consul = ConsulClient::new(&config.server.consul_host);
    // if let Err(e) = consul.put_config(CONSUL_CLUSTER_KEY, &config).await {
    //     eprintln!("Failed to write cluster config to Consul: {}", e);
    // } else {
    //     println!("Cluster config registered in Consul");
    // }

    // Build TLS configuration
    let tls_config = match tlsmod::build_server_tls(&config).await {
        Ok(cfg) => {
            println!("TLS configuration loaded successfully");
            cfg
        }
        Err(e) => {
            eprintln!("Failed to load TLS configuration: {}", e);
            std::process::exit(1);
        }
    };

    let (iops_tls_config, _ca_cert) = match tlsmod::build_interops_tls(&config).await {
        Ok((cfg, cert)) => {
            println!("Interops TLS configuration loaded successfully");
            (cfg, cert)
        }
        Err(e) => {
            eprintln!("Failed to load Interops TLS configuration: {}", e);
            std::process::exit(1);
        }
    };

    let shared_data: SharedData = Arc::new(Mutex::new(SharedState::new(config.clone())));

    let interops_service = InteropsService::new(Arc::clone(&shared_data));
    let warehouse_service = WarehouseService::initialize(Arc::clone(&shared_data)).await;
    // reflection descriptor bytes will be passed to handlers to build the reflection service
    let reflection_descriptor: &'static [u8] =
        include_bytes!("../../kionas/generated/grpc.reflection.v1alpha");

    let security_info = match config.services.security.clone() {
        Some(s) => s,
        None => {
            eprintln!("Missing security configuration under services.security");
            std::process::exit(1);
        }
    };

    let jwt_secret = security_info.secret.clone();
    let data_path = security_info.data_path.clone();

    // Initialize authentication related pieces
    let (jwt_interceptor, auth_service) = match auth_setup::initialize_auth(
        crate::warehouse::state::SharedData::default(),
        jwt_secret.clone(),
        data_path.clone(),
    )
    .await
    {
        Ok((jwt, auth)) => {
            println!("Authentication initialized successfully");
            (jwt, auth)
        }
        Err(e) => {
            eprintln!("Failed to initialize authentication: {}", e);
            std::process::exit(1);
        }
    };

    let warehouse_cfg = match config.services.warehouse.as_ref() {
        Some(w) => w,
        None => {
            eprintln!("Missing services.warehouse configuration");
            std::process::exit(1);
        }
    };
    let interops_cfg = match config.services.interops.as_ref() {
        Some(i) => i,
        None => {
            eprintln!("Missing services.interops configuration");
            std::process::exit(1);
        }
    };

    let warehouse_addr = resolve_hostname(&warehouse_cfg.host, warehouse_cfg.port).await?;
    let interops_addr = resolve_hostname(&interops_cfg.host, interops_cfg.port).await?;

    // Build the server futures for warehouse and interops
    let (warehouse_server, interops_server) = handlers::build_servers(
        tls_config,
        iops_tls_config,
        warehouse_addr,
        interops_addr,
        reflection_descriptor,
        auth_service,
        jwt_interceptor,
        warehouse_service,
        interops_service,
    );

    // Start periodic janitor tasks for Redis-backed dashboard snapshots.
    janitor::start(Arc::clone(&shared_data));

    let address = warehouse_cfg.host.clone();
    let port = warehouse_cfg.port.clone();
    log::info!("Starting server on {}:{}", address, port);
    print_server_info();
    print_memory_usage();

    tokio::try_join!(warehouse_server, interops_server)?;
    Ok(())
}
