use crate::error::{Error, Result};
use crate::message::{Message, RaftResponse};
use crate::raft_node::Peer;
use crate::raft_node::RaftNode;
use crate::raft_server::RaftServer;
use crate::raft_service::raft_service_client::RaftServiceClient;
use crate::raft_service::ResultCode;
use tokio::task::JoinHandle;

use async_trait::async_trait;
use bincode::{deserialize, serialize};
use log::{info, warn};
use raft::eraftpb::{ConfChange, ConfChangeType};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tonic::Request;

use std::time::Duration;

#[async_trait]
pub trait Store {
    async fn apply(&mut self, message: &[u8]) -> Result<Vec<u8>>;
    async fn snapshot(&self) -> Result<Vec<u8>>;
    async fn restore(&mut self, snapshot: &[u8]) -> Result<()>;
}

/// A mailbox to send messages to a ruung raft node.
#[derive(Clone)]
pub struct Mailbox(mpsc::Sender<Message>);

impl Mailbox {
    /// sends a proposal message to commit to the node. This fails if the current node is not the
    /// leader
    pub async fn send(&self, message: Vec<u8>) -> Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        let proposal = Message::Propose {
            proposal: message,
            chan: tx,
        };
        let sender = self.0.clone();
        // TODO make timeout duration a variable
        match sender.send(proposal).await {
            Ok(_) => match timeout(Duration::from_secs(2), rx).await {
                Ok(Ok(RaftResponse::Response { data })) => Ok(data),
                _ => Err(Error::Unknown),
            },
            _ => Err(Error::Unknown),
        }
    }

    pub async fn leave(&self) -> Result<()> {
        let mut change = ConfChange::default();
        // set node id to 0, the node will set it to self when it receives it.
        change.set_node_id("".to_string());
        change.set_change_type(ConfChangeType::RemoveNode);
        let sender = self.0.clone();
        let (chan, rx) = oneshot::channel();
        match sender.send(Message::ConfigChange { change, chan }).await {
            Ok(_) => match rx.await {
                Ok(RaftResponse::Ok) => Ok(()),
                _ => Err(Error::Unknown),
            },
            _ => Err(Error::Unknown),
        }
    }
}

pub struct Raft<S: Store + 'static> {
    store: S,
    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
    addr: String,
    logger: slog::Logger,
}

impl<S: Store + Send + Sync + 'static> Raft<S> {
    /// creates a new node with the given address and store.
    pub fn new(addr: &str, store: S, logger: slog::Logger) -> Self {
        let (tx, rx) = mpsc::channel(100);
        Self {
            store,
            tx,
            rx,
            addr: addr.to_string(),
            logger,
        }
    }

    /// gets the node's `Mailbox`.
    pub fn mailbox(&self) -> Mailbox {
        Mailbox(self.tx.clone())
    }

    /// Create a new leader for the cluster. There has to be exactly one node in the
    /// cluster that is initialised that way
    pub async fn lead(self) -> Result<JoinHandle<Result<()>>> {
        let addr = self.addr.clone();
        let node = RaftNode::new_leader(
            &self.addr,
            self.rx,
            self.tx.clone(),
            self.store,
            &self.logger,
        );
        let server = RaftServer::new(self.tx, addr);
        let _server_handle = tokio::spawn(server.run());
        let node_handle = tokio::spawn(node.run());
        Ok(node_handle)
    }

    /// Try using node addr as an id to join cluster at `addr`, or finding it if
    /// `addr` is not the current leader of the cluster, it will use returned new addr
    /// join will try to remove this node and add again
    pub async fn join(self, addr: &str) -> Result<JoinHandle<Result<()>>> {
        info!("attemping to join cluster using peer: {}", &addr);
        let server = RaftServer::new(self.tx.clone(), self.addr.clone());
        let _server_handle = tokio::spawn(server.run());
        let node = RaftNode::new_follower(
            &self.addr,
            self.rx,
            self.tx.clone(),
            self.store,
            &self.logger,
        )
        .unwrap();
        let mut leader_addr = addr.to_string();
        let mut change = ConfChange::default();
        change.set_node_id(self.addr.clone());
        // try to remove node firstly
        change.set_change_type(ConfChangeType::RemoveNode);
        change.set_context(serialize(&self.addr)?);
        loop {
            if let Ok(mut client) =
                RaftServiceClient::connect(format!("http://{}", &leader_addr)).await
            {
                let response = client
                    .change_config(Request::new(change.clone()))
                    .await?
                    .into_inner();
                match response.code() {
                    ResultCode::WrongLeader => {
                        let addr: String = deserialize(&response.data)?;
                        leader_addr = addr;
                        info!("rejoin with new peer at {}", leader_addr);
                        continue;
                    }
                    ResultCode::Ok => {
                        if change.change_type == ConfChangeType::AddNode as i32 {
                            info!("join successfully with leader at {}", leader_addr);
                            let node_handle = tokio::spawn(node.run());
                            return Ok(node_handle);
                        } else if change.change_type == ConfChangeType::RemoveNode as i32 {
                            // removed node, add node again
                            change.set_change_type(ConfChangeType::AddNode);
                            continue;
                        }
                    }
                    ResultCode::Error => continue,
                }
            }
        }
    }
}
