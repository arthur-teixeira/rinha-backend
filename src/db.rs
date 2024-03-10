use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::{AccountExtract, Transaction};

pub struct Connection {
    stream: TcpStream,
}

pub const TRANSACTION_ROW_SIZE: usize = 23;

impl Connection {
    pub async fn connect(port: String) -> io::Result<Self> {
        let addr = format!("127.0.0.1:{}", port);

        let stream = TcpStream::connect(addr).await?;
        Ok(Connection { stream })
    }

    pub async fn insert_transaction(
        &mut self,
        transaction: &crate::Transaction,
        account_id: i32,
    ) -> io::Result<i32> {
        let mut buf: Vec<u8> = Vec::new();
        buf.push(b'i');
        buf.push(account_id as u8);
        buf.extend(match transaction.tipo {
            crate::TransactionType::Credit => b'c'.to_be_bytes(),
            crate::TransactionType::Debit => b'd'.to_be_bytes(),
        });
        buf.extend(transaction.valor.to_be_bytes().to_vec());
        buf.extend(transaction.descricao.0.as_bytes());

        self.stream.write_all(&buf).await?;
        self.stream.flush().await?;

        self.stream.readable().await?;
        let success = self.stream.read_u8().await?;

        self.stream.readable().await?;
        let balance = self.stream.read_i32().await?;

        if success == 1 {
            Err(io::Error::new(io::ErrorKind::InvalidData, "Transação inválida"))
        } else {
            Ok(balance)
        }
    }

    pub async fn get_transactions(&mut self, account_id: i32) -> io::Result<AccountExtract> {
        let mut buf: [u8; 2] = [b'g', 0];
        buf[1] = account_id as u8;

        self.stream.write_all(&buf).await?;
        self.stream.flush().await?;

        self.stream.readable().await?;
        let response_size = self.stream.read_i32().await?;

        self.stream.readable().await?;
        let balance = self.stream.read_i32().await?;

        if response_size == 0 {
            return Ok(AccountExtract {
                transactions: Vec::new(),
                balance,
            });
        }

        let mut response = Vec::with_capacity(response_size as usize);

        loop {
            self.stream.readable().await?;
            match self.stream.read_buf(&mut response).await {
                Ok(_) => break,
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        let result = Transaction::from_binary_array(response);

        match result {
            Ok(transactions) => Ok(AccountExtract {
                transactions,
                balance,
            }),
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        }
    }
}
