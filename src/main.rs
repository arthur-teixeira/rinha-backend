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
use std::sync::Arc;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::sync::RwLock;

mod db;
mod pool;

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
    valor: i32,
    tipo: TransactionType,
    descricao: Description,
    #[serde(with = "time::serde::rfc3339", default = "OffsetDateTime::now_utc")]
    realizada_em: OffsetDateTime,
}

impl Transaction {
    fn from_binary_array(data: Vec<u8>) -> Result<Vec<Self>, &'static str> {
        let amount = data.len() / db::TRANSACTION_ROW_SIZE;

        let mut result = Vec::with_capacity(amount);
        for i in 0..amount {
            let start = i * db::TRANSACTION_ROW_SIZE;
            let end = start + db::TRANSACTION_ROW_SIZE;
            let row = &data[start..end];

            result.push(Self::from_binary(row)?);
        }

        result.sort_by(|a, b| b.realizada_em.cmp(&a.realizada_em));

        Ok(result)
    }

    fn from_binary(data: &[u8]) -> Result<Self, &'static str> {
        let mut valor_buf = [0u8; 4];
        valor_buf.copy_from_slice(&data[0..4]);
        let valor = i32::from_be_bytes(valor_buf);

        let mut descricao = Vec::with_capacity(10);
        descricao.extend_from_slice(&data[4..14]);
        descricao.retain(|&x| x != 0);

        let tipo = data[14];

        let mut realizada_em_buf = [0u8; 8];
        realizada_em_buf.copy_from_slice(&data[15..23]);
        let realizada_em = i64::from_be_bytes(realizada_em_buf);

        Ok(Self {
            valor,
            tipo: match tipo {
                b'c' => TransactionType::Credit,
                b'd' => TransactionType::Debit,
                _ => return Err("Tipo de transação inválido"),
            },
            descricao: Description(String::from_utf8(descricao).unwrap()),
            realizada_em: match OffsetDateTime::from_unix_timestamp(realizada_em) {
                Ok(dt) => dt,
                Err(_) => return Err("Data de transação inválida"),
            },
        })
    }
}

struct Account {
    limit: i32,
    id: i32,
}

#[derive(Serialize)]
struct TransactionResult {
    saldo: i32,
    limite: i32,
}

struct AccountExtract {
    balance: i32,
    transactions: Vec<Transaction>,
}

impl Account {
    fn new(limit: i32, id: i32) -> Self {
        Account { limit, id }
    }

    async fn do_transaction(
        &mut self,
        t: Transaction,
        db: &pool::Pool,
    ) -> Result<TransactionResult, String> {
        let mut conn = match db.get().await {
            Ok(conn) => conn,
            Err(_) => return Err("Erro ao buscar conexão no pool".to_string()),
        };

        let new_bal = match conn.insert_transaction(&t, self.id).await {
            Ok(bal) => bal,
            Err(e) => {
                let msg = format!("Erro ao inserir transação no banco: {e}");
                return Err(msg);
            }
        };

        let result = TransactionResult {
            saldo: new_bal,
            limite: self.limit,
        };

        Ok(result)
    }

    async fn transactions(&self, db: &pool::Pool) -> Result<AccountExtract, &str> {
        let mut conn = match db.get().await {
            Ok(conn) => conn,
            Err(_) => return Err("Erro ao buscar conexão no pool"),
        };

        let result = match conn.get_transactions(self.id).await {
            Ok(result) => Ok(result),
            Err(_) => return Err("Erro ao buscar transações no banco"),
        };

        result
    }
}

type Accounts = HashMap<u8, RwLock<Account>>;

struct AppState {
    accounts: Accounts,
    db: pool::Pool,
}

impl AppState {
    async fn new(accounts: Accounts) -> Self {
        let manager = pool::ConnectionPool {};
        let db = pool::Pool::builder(manager).build().unwrap();
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

    let acc1 = Account::new(100_000, 1);
    let acc2 = Account::new(80_000, 2);
    let acc3 = Account::new(1_000_000, 3);
    let acc4 = Account::new(10_000_000, 4);
    let acc5 = Account::new(500_000, 5);

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

async fn create_transaction(
    Path(id): Path<u8>,
    State(app_state): State<Arc<AppState>>,
    Json(transaction): Json<Transaction>,
) -> impl IntoResponse {
    match app_state.accounts.get(&id) {
        Some(acc) => {
            let mut account = acc.write().await;
            match account.do_transaction(transaction, &app_state.db).await {
                Ok(result) => Ok(Json(json!(result))),
                Err(_) =>return Err(StatusCode::UNPROCESSABLE_ENTITY),
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
            if let Ok(result) = account.transactions(&app_state.db).await {
                Ok(Json(json!({
                    "saldo": {
                        "total": result.balance,
                        "data_extrato": OffsetDateTime::now_utc().format(&Rfc3339).unwrap(),
                        "limite": account.limit,
                    },
                    "ultimas_transacoes": result.transactions,
                })))
            } else {
                Err(StatusCode::NOT_FOUND)
            }
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}
