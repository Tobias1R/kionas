/*
A client for redis
 */

use redis::AsyncCommands;
use redis::Client;
use redis::Commands;

#[derive(Clone, Debug)]
pub struct SessionProvider {
    client: Client,
}

impl SessionProvider {
    pub fn new(url: &str) -> SessionProvider {
        let client = Client::open(url).expect("Redis client creation error");
        SessionProvider { client }
    }

    pub async fn set(&self, key: &str, value: &str) -> redis::RedisResult<()> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        con.set(key, value).await
    }

    pub async fn get(&self, key: &str) -> redis::RedisResult<String> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        con.get(key).await
    }

    pub async fn del(&self, key: &str) -> redis::RedisResult<()> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        con.del(key).await
    }

    pub async fn exists(&self, key: &str) -> redis::RedisResult<bool> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        con.exists(key).await
    }

    pub async fn expire(&self, key: &str, seconds: i64) -> redis::RedisResult<bool> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        con.expire(key, seconds).await
    }

    // keys
    pub async fn keys(&self, pattern: &str) -> redis::RedisResult<Vec<String>> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        con.keys(pattern).await
    }
}

pub struct SyncSessionProvider {
    con: redis::Connection,
}

impl SyncSessionProvider {
    pub fn new(url: &str) -> SyncSessionProvider {
        let con = redis::Client::open(url)
            .expect("Redis client creation error")
            .get_connection()
            .unwrap();
        SyncSessionProvider { con }
    }

    pub fn set(&mut self, key: &str, value: &str) -> redis::RedisResult<()> {
        self.con.set(key, value)
    }

    pub fn get(&mut self, key: &str) -> redis::RedisResult<String> {
        self.con.get(key)
    }

    pub fn del(&mut self, key: &str) -> redis::RedisResult<()> {
        self.con.del(key)
    }

    pub fn exists(&mut self, key: &str) -> redis::RedisResult<bool> {
        self.con.exists(key)
    }

    pub fn expire(&mut self, key: &str, seconds: i64) -> redis::RedisResult<bool> {
        self.con.expire(key, seconds)
    }

    // keys
    pub fn keys(&mut self, pattern: &str) -> redis::RedisResult<Vec<String>> {
        self.con.keys(pattern)
    }
}

/*
Kionas Session

This object hold all the information about
*/
