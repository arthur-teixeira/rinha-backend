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
use sqlx::Postgres;
use sqlx::{postgres::PgPoolOptions, Pool};
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

    async fn do_transaction(&mut self, t: Transaction, db: &Pool<Postgres>) -> Result<(), &str> {
        let bal = match t.tipo {
            TransactionType::Credit => t.valor + self.balance,
            TransactionType::Debit => {
                if self.limit + self.balance < t.valor {
                    return Err("O valor ultrapassa o limite da conta");
                }

                self.balance - t.valor
            }
        };

        let mut tx = match db.begin().await {
            Ok(tx) => tx,
            Err(_) => return Err("Erro ao iniciar transação"),
        };

        match sqlx::query(
            "INSERT INTO transacoes (cliente_id, valor, descricao, tipo) VALUES ($1, $2, $3, $4)",
        )
        .bind(1)
        .bind(t.valor as i16)
        .bind(t.descricao.0.clone())
        .bind(match t.tipo {
            TransactionType::Credit => "c",
            TransactionType::Debit => "d",
        })
        .execute(&mut *tx)
        .await
        {
            Ok(_) => (),
            Err(_) => return Err("Erro ao inserir transação"),
        }

        self.balance = bal;
        self.transactions.push(t);

        match tx.commit().await {
            Ok(_) => Ok(()),
            Err(_) => Err("Erro ao finalizar transação"),
        }
    }

    async fn transactions(&self, id: i16, db: &Pool<Postgres>) -> Result<Vec<Transaction>, &str> {
        let mut tx = match db.begin().await {
            Ok(tx) => tx,
            Err(_) => return Err("Erro ao iniciar transação"),
        };

        let rows = match sqlx::query("SELECT valor, descricao, tipo, realizada_em FROM transacoes WHERE cliente_id = ($1) ORDER BY realizada_em DESC LIMIT 10")
            .bind(id)
            .fetch_all(&mut *tx)
            .await
            {
                Ok(rows) => rows,
                Err(_) => return Err("Erro ao buscar transações"),
            };

        let mut transactions = Vec::with_capacity(10);
        for row in rows {
        }

        Err("Not implemented")
    }
}

type Accounts = HashMap<u8, RwLock<Account>>;

struct AppState {
    accounts: Accounts,
    db: Pool<Postgres>,
}

impl AppState {
    async fn new(accounts: Accounts) -> Self {
        let db = connect_to_db().await;
        AppState { accounts, db }
    }
}

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
    let app_state = Arc::new(AppState::new(accounts).await);

    let app = Router::new()
        .route("/clientes/:id/transacoes", post(create_transaction))
        .route("/clientes/:id/extrato", get(get_transactions))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .unwrap();

    axum::serve(listener, app).await.unwrap();
}

async fn connect_to_db() -> Pool<Postgres> {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL not found");
    PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .expect("Failed to connect to database")
}

async fn create_transaction(
    Path(id): Path<u8>,
    State(app_state): State<Arc<AppState>>,
    Json(transaction): Json<Transaction>,
) -> impl IntoResponse {
    match app_state.accounts.get(&id) {
        Some(acc) => {
            let mut account = acc.write().await;
            match account.do_transaction(transaction, &app_state.db).await {
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
    State(app_state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match app_state.accounts.get(&id) {
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
