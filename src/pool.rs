use std::io;
use async_trait::async_trait;
use deadpool::managed;


use crate::db::Connection;

pub struct ConnectionPool { }

#[async_trait]
impl managed::Manager for ConnectionPool {
    type Type = Connection;
    type Error = io::Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        Connection::connect("8080".into()).await
    }

    async fn recycle(&self, _: &mut Self::Type, _: &managed::Metrics) -> managed::RecycleResult<Self::Error> {
        Ok(())
    }
}

pub type Pool = managed::Pool<ConnectionPool>;
