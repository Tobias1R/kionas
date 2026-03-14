pub mod utils;
pub mod logging;
pub mod session;
pub mod parser;
pub mod consul;
pub mod constants;

use uuid::Uuid;
use chrono::Utc;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;
use md5;
use get_if_addrs::get_if_addrs;
use std::net::IpAddr;
use hostname; 

use regex::Regex;
use std::env;

#[derive(Debug, Clone)]
pub enum RedisDatabases {
    Session = 0,
    Config = 1,
    Status = 2
}

impl RedisDatabases {
    pub const SESSION: u8 = 0;
    pub const CONFIG: u8 = 1;
    pub const STATUS: u8 = 2;

    fn index(&self) -> u8 {
        match self {
            RedisDatabases::Session => RedisDatabases::SESSION,
            RedisDatabases::Config => RedisDatabases::CONFIG,
            RedisDatabases::Status => RedisDatabases::STATUS,
        }
    }
}

pub fn get_redis_url(database_index: u8) -> String {
    format!("redis://redis:6379/{}", database_index)
}

pub fn get_redis_url_status() -> String {
    get_redis_url(RedisDatabases::STATUS)
}

pub fn get_redis_url_config() -> String {
    get_redis_url(RedisDatabases::CONFIG)
}

pub fn get_redis_url_session() -> String {
    get_redis_url(RedisDatabases::SESSION)
}





pub fn get_local_ip() -> Option<IpAddr> {
    let if_addrs = get_if_addrs().unwrap();
    for iface in if_addrs {
        if iface.is_loopback() {
            return Some(iface.ip());
        }
    }
    None
}

pub fn parse_env_vars(input: &str) -> String {
    let re = Regex::new(r"\$\{(\w+)\}").unwrap();
    re.replace_all(input, |caps: &regex::Captures| {
        let var_name = &caps[1];
        env::var(var_name).unwrap_or_else(|_| format!("${{{}}}", var_name))
    }).to_string()
}

/// Generate a UUID based on the current timestamp and a string parameter.
pub fn gen_uuid_based_on_timestamp_and_string(param: &str) -> String {
    let timestamp = Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!("{}{}", timestamp, param);
    let hash = md5::compute(data);
    Uuid::from_bytes(*hash).to_string()
}

/// Generate a random UUID (version 4).
pub fn gen_random_uuid() -> String {
    Uuid::new_v4().to_string()
}

// Get digest of a string
pub fn get_digest(data: &str) -> String {
    let hash = md5::compute(data);
    Uuid::from_bytes(*hash).to_string()
}

/// Read a binary file and return its contents as a vector of bytes.
pub fn read_bin_file<P: AsRef<Path>>(path: P) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    Ok(buffer)
}

/// Write a vector of bytes to a binary file.
pub fn dump_bin_file<P: AsRef<Path>>(path: P, data: &[u8]) -> io::Result<()> {
    let mut file = File::create(path)?;
    file.write_all(data)?;
    Ok(())
}

// Compute execution time
pub fn compute_execution_time(start_time: chrono::DateTime<Utc>) -> i64 {
    let end_time = Utc::now();
    let duration = end_time.signed_duration_since(start_time);
    duration.num_milliseconds()
}

// get local hostname
pub fn get_local_hostname() -> Option<String> {
    hostname::get().ok().and_then(|h| h.into_string().ok())
}