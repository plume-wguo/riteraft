#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use slog::Drain;

use actix_web::{get, web, App, HttpServer, Responder};
use async_trait::async_trait;
use bincode::{deserialize, serialize};
use riteraft::{Mailbox, Raft, Result, Store};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{mpsc, Arc, RwLock};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Options {
    #[structopt(long)]
    raft_addr: String,
    #[structopt(long)]
    peer_addr: Option<String>,
    #[structopt(long)]
    web_server: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub enum Message {
    Insert { key: u64, value: String },
}

#[derive(Clone)]
struct HashStore(Arc<RwLock<HashMap<u64, String>>>);

impl HashStore {
    fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }
    fn get(&self, id: u64) -> Option<String> {
        self.0.read().unwrap().get(&id).cloned()
    }
}

#[async_trait]
impl Store for HashStore {
    async fn apply(&mut self, message: &[u8]) -> Result<Vec<u8>> {
        let message: Message = deserialize(message).unwrap();
        let message: Vec<u8> = match message {
            Message::Insert { key, value } => {
                let mut db = self.0.write().unwrap();
                db.insert(key, value.clone());
                log::info!("inserted: ({}, {})", key, value);
                serialize(&value).unwrap()
            }
        };
        Ok(message)
    }

    async fn snapshot(&self) -> Result<Vec<u8>> {
        Ok(serialize(&self.0.read().unwrap().clone())?)
    }

    async fn restore(&mut self, snapshot: &[u8]) -> Result<()> {
        let new: HashMap<u64, String> = deserialize(snapshot).unwrap();
        let mut db = self.0.write().unwrap();
        let _ = std::mem::replace(&mut *db, new);
        Ok(())
    }
}

#[get("/put/{id}/{name}")]
async fn put(
    data: web::Data<(Arc<Mailbox>, HashStore)>,
    path: web::Path<(u64, String)>,
) -> impl Responder {
    let message = Message::Insert {
        key: path.0,
        value: path.1.clone(),
    };
    let message = serialize(&message).unwrap();
    let result = data.0.propose(message).await.unwrap();
    let result: String = deserialize(&result).unwrap();
    format!("{:?}", result)
}

#[get("/get/{id}")]
async fn get(data: web::Data<(Arc<Mailbox>, HashStore)>, path: web::Path<u64>) -> impl Responder {
    let id = path.into_inner();

    let response = data.1.get(id);
    format!("{:?}", response)
}

#[get("/leave")]
async fn leave(data: web::Data<(Arc<Mailbox>, HashStore)>) -> impl Responder {
    data.0.leave().await.unwrap();
    "OK".to_string()
}

#[actix_rt::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, slog_o!("version" => env!("CARGO_PKG_VERSION")));

    // converts log to slog
    let _scope_guard = slog_scope::set_global_logger(logger.clone());
    let _log_guard = slog_stdlog::init_with_level(log::Level::Debug).unwrap();

    let options = Options::from_args();
    let store = HashStore::new();

    let (tx, rx) = tokio::sync::mpsc::channel(100);
    let raft = Raft::new(&options.raft_addr, store.clone(), tx, logger.clone());
    let mailbox = Arc::new(raft.mailbox());
    let raft_handle = match options.peer_addr {
        Some(addr) => {
            log::info!("running in follower mode");
            raft.join(vec![&addr]).await?
        }
        None => {
            log::info!("running in leader mode");
            raft.lead().await?
        }
    };
    // let raft_handle = tokio::spawn(node.run());

    if let Some(addr) = options.web_server {
        let _server = tokio::spawn(
            HttpServer::new(move || {
                App::new()
                    .app_data(web::Data::new((mailbox.clone(), store.clone())))
                    .service(put)
                    .service(get)
                    .service(leave)
            })
            .bind(addr)
            .unwrap()
            .run(),
        );
    }

    let result = tokio::try_join!(raft_handle)?;
    result.0?;
    Ok(())
}
