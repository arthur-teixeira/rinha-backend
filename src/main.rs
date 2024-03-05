use std::{collections::HashMap, env};

use axum::extract::State;
use axum::http::StatusCode;
use axum::{
    extract::Path,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{self, Deserialize, Serialize};
use serde_json::json;
use std::collections::VecDeque;
use std::sync::Arc;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::sync::RwLock;

#[derive(Clone, Serialize, Deserialize)]
enum TransactionType {
    #[serde(rename = "c")]
    Credit,
    #[serde(rename = "d")]
    Debit,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(try_from = "String")]
struct Description(String);

impl TryFrom<String> for Description {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() || value.len() > 10 {
            return Err("Descrição inválida");
        }

        Ok(Description(value))
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Transaction {
    valor: isize,
    tipo: TransactionType,
    descricao: Description,
    #[serde(with = "time::serde::rfc3339", default = "OffsetDateTime::now_utc")]
    realizada_em: OffsetDateTime,
}

#[derive(Clone, Serialize)]
struct RingBuffer<T> {
    buf: VecDeque<T>,
}

impl<T> Default for RingBuffer<T> {
    fn default() -> Self {
        RingBuffer::with_capacity(10)
    }
}

impl<T> RingBuffer<T> {
    fn with_capacity(cap: usize) -> Self {
        RingBuffer {
            buf: VecDeque::with_capacity(cap),
        }
    }

    pub fn push(&mut self, item: T) {
        if self.buf.len() == self.buf.capacity() {
            self.buf.pop_back();
            self.buf.push_front(item);
        } else {
            self.buf.push_front(item);
        }
    }
}

struct Account {
    balance: isize,
    limit: isize,
    transactions: RingBuffer<Transaction>,
}

impl Account {
    fn new(limit: isize) -> Self {
        Account {
            balance: 0,
            limit,
            transactions: RingBuffer::default(),
        }
    }

    fn do_transaction(&mut self, t: Transaction) -> Result<(), &str> {
        let bal = match t.tipo {
            TransactionType::Credit => t.valor + self.balance,
            TransactionType::Debit => {
                if self.limit + self.balance < t.valor {
                    return Err("O valor ultrapassa o limite da conta");
                }

                self.balance - t.valor
            }
        };

        self.balance = bal;

        self.transactions.push(t);

        Ok(())
    }
}

type Accounts = HashMap<u8, RwLock<Account>>;
type AppState = Arc<Accounts>;

#[tokio::main]
async fn main() {
    println!("Starting server...");

    let port = env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3000);

    let acc1 = Account::new(100_000);
    let acc2 = Account::new(80_000);
    let acc3 = Account::new(1_000_000);
    let acc4 = Account::new(10_000_000);
    let acc5 = Account::new(500_000);

    let accounts = Accounts::from_iter([
        (1, RwLock::new(acc1)),
        (2, RwLock::new(acc2)),
        (3, RwLock::new(acc3)),
        (4, RwLock::new(acc4)),
        (5, RwLock::new(acc5)),
    ]);

    let app = Router::new()
        .route("/clientes/:id/transacoes", post(create_transaction))
        .route("/clientes/:id/extrato", get(get_transactions))
        .with_state(Arc::new(accounts));

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .unwrap();

    axum::serve(listener, app).await.unwrap();
}

async fn create_transaction(
    Path(id): Path<u8>,
    State(accounts): State<AppState>,
    Json(transaction): Json<Transaction>,
) -> impl IntoResponse {
    match accounts.get(&id) {
        Some(acc) => {
            let mut account = acc.write().await;
            match account.do_transaction(transaction) {
                Ok(()) => Ok(Json(json!({
                    "limite": account.limit,
                    "saldo": account.balance,
                }))),
                Err(_) => return Err(StatusCode::UNPROCESSABLE_ENTITY),
            }
        }
        None => return Err(StatusCode::NOT_FOUND),
    }
}

async fn get_transactions(
    Path(id): Path<u8>,
    State(accounts): State<AppState>,
) -> impl IntoResponse {
    match accounts.get(&id) {
        Some(acc) => {
            let account = acc.read().await;
            Ok(Json(json!({
                "saldo": {
                    "total": account.balance,
                    "data_extrato": OffsetDateTime::now_utc().format(&Rfc3339).unwrap(),
                    "limite": account.limit,
                },
                "ultimas_transacoes": account.transactions.buf,
            })))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}
