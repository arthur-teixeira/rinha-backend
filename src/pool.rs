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

// impl ConnectionPool {
//     pub async fn new(max_connections: usize, port: String) -> io::Result<Self> {
//         let mut connections = VecDeque::with_capacity(max_connections);
//         for _ in 0..max_connections {
//             let conn = Connection::connect(port.clone()).await?;
//             connections.push_back(conn);
//         }
//
//         Ok(ConnectionPool {
//             connections: Mutex::new(connections),
//             semaphore: Semaphore::new(max_connections),
//         })
//     }
//
//     pub async fn get_connection(&self) -> Result<Connection, &'static str> {
//         let _permit = self.semaphore.acquire().await.unwrap();
//         let mut connections = self.connections.lock().unwrap();
//
//         match connections.pop_front() {
//             Some(conn) => Ok(conn),
//             None => Err( "Sem conexões disponíveis"),
//         }
//     }
//
//     pub async fn release_connection(&self, conn: Connection) {
//         let mut connections = self.connections.lock().unwrap();
//         connections.push_back(conn);
//         drop(connections);
//         self.semaphore.add_permits(1);
//     }
// }
