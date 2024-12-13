use crate::error::{Error, Result};
use crate::message::{Message, RaftRole, Response};
use crate::raft_node::{Peer, RaftNode};
use crate::raft_server::RaftServer;
use crate::raft_service::raft_service_client::RaftServiceClient;
use crate::raft_service::ResultCode;
use async_trait::async_trait;
use bincode::{deserialize, serialize};
use log::{debug, error, info};
use raft::eraftpb::{ConfChange, ConfChangeType};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tonic::Request;

use std::time::Duration;

#[derive(Clone)]
pub struct ServiceProposalRequest {
    pub inner: Vec<u8>,
}

#[async_trait]
pub trait Store {
    async fn apply(&mut self, message: &[u8]) -> Result<Vec<u8>>;
    async fn snapshot(&self) -> Result<Vec<u8>>;
    async fn restore(&mut self, snapshot: &[u8]) -> Result<()>;
}

/// A mailbox to send messages to a local raft node.
/// to send message to remote raft node, use MessageSender
#[derive(Clone)]
pub struct Mailbox(mpsc::Sender<Message>);

impl Mailbox {
    /// sends a Propose message to commit to local raft node. This fails if the local node is not the leader
    pub async fn propose(&self, message: Vec<u8>) -> Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        let proposal = Message::Propose {
            proposal: message,
            reply_chan: tx,
        };
        let sender = self.0.clone();
        // TODO make timeout duration a variable
        match sender.send(proposal).await {
            Ok(_) => match timeout(Duration::from_secs(2), rx).await {
                Ok(Ok(Response::Response { data })) => Ok(data),
                Ok(Ok(Response::WrongLeader { leader_addr })) => {
                    Err(Error::WrongLeader(leader_addr))
                }
                _ => Err(Error::Unknown),
            },
            _ => Err(Error::Unknown),
        }
    }

    pub async fn leave(&self) -> Result<()> {
        let sender = self.0.clone();
        let (tx, rx) = oneshot::channel();
        match sender.send(Message::Leave { reply_chan: tx }).await {
            Ok(_) => match rx.await {
                Ok(Response::Ok {}) => Ok(()),
                _ => Err(Error::Unknown),
            },
            _ => Err(Error::Unknown),
        }
    }

    pub async fn role(&self) -> Result<(RaftRole, String)> {
        let sender = self.0.clone();
        let (tx, rx) = oneshot::channel();
        match sender.send(Message::RaftState { reply_chan: tx }).await {
            Ok(_) => match rx.await {
                Ok(Response::RaftState { role, leader_id }) => Ok((role, leader_id)),
                _ => Err(Error::Unknown),
            },
            _ => Err(Error::Unknown),
        }
    }

    pub async fn request_service_proposal(&self, request: Vec<u8>) -> Result<()> {
        let sender = self.0.clone();
        match sender
            .send(Message::ServiceProposalRequest { request })
            .await
        {
            Ok(_) => Ok(()),
            _ => Err(Error::Unknown),
        }
    }
}

pub struct Raft<S: Store + 'static> {
    store: S,
    proposal_service_tx: mpsc::Sender<ServiceProposalRequest>,
    raft_node_tx: mpsc::Sender<Message>,
    raft_node_rx: mpsc::Receiver<Message>,
    addr: String,
    logger: slog::Logger,
}

impl<S: Store + Send + Sync + 'static> Raft<S> {
    /// creates a new node with the given address and store.
    pub fn new(
        addr: &str,
        store: S,
        proposal_service_tx: mpsc::Sender<ServiceProposalRequest>,
        logger: slog::Logger,
    ) -> Self {
        let (tx, rx) = mpsc::channel(100);
        Self {
            store,
            proposal_service_tx,
            raft_node_tx: tx,
            raft_node_rx: rx,
            addr: addr.to_string(),
            logger,
        }
    }

    /// gets the node's `Mailbox`.
    pub fn mailbox(&self) -> Mailbox {
        Mailbox(self.raft_node_tx.clone())
    }

    /// Create a new leader for the cluster. There has to be exactly one node in the
    /// cluster that is initialised that way
    pub async fn lead(self) -> Result<JoinHandle<Result<()>>> {
        // pub async fn lead(self) -> Result<RaftNode<S>> {
        let addr = self.addr.clone();
        let node = RaftNode::new_leader(
            &self.addr,
            self.raft_node_rx,
            self.raft_node_tx.clone(),
            self.store,
            &self.logger,
        );
        let server = RaftServer::new(self.raft_node_tx, self.proposal_service_tx, addr);
        let _server_handle = tokio::spawn(server.run());
        let node_handle = tokio::spawn(node.run());
        Ok(node_handle)
        // Ok(node)
    }

    /// Try using node addr as an id to join cluster at `peers`, or finding it if
    /// `peers` is not the current leader of the cluster, it will use returned new addr
    /// join will try to remove this node and add again
    pub async fn join(self, peers: Vec<&str>) -> Result<JoinHandle<Result<()>>> {
        // pub async fn join(self, peers: Vec<&str>) -> Result<RaftNode<S>> {
        info!(
            "{} attemping to join cluster with peers: {:?}",
            &self.addr, &peers
        );
        let server = RaftServer::new(
            self.raft_node_tx.clone(),
            self.proposal_service_tx,
            self.addr.clone(),
        );
        let _server_handle = tokio::spawn(server.run());
        let node = RaftNode::new_follower(
            &self.addr,
            self.raft_node_rx,
            self.raft_node_tx.clone(),
            self.store,
            &self.logger,
        )
        .unwrap();
        let handle = tokio::spawn(node.run());
        let mut change = ConfChange::default();
        change.set_node_id(self.addr.clone());
        // try to remove node firstly
        change.set_change_type(ConfChangeType::RemoveNode);
        change.set_context(serialize(&self.addr)?);
        for addr in peers.clone() {
            let mut leader_addr = addr.to_string();
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                match RaftServiceClient::connect(format!("http://{}", &leader_addr)).await {
                    Ok(mut client) => {
                        let response = client
                            .change_config(Request::new(change.clone()))
                            .await?
                            .into_inner();
                        info!("received change_config api response: {}", &response.code);
                        match response.code() {
                            ResultCode::WrongLeader => {
                                let addr: String = deserialize(&response.data)?;
                                leader_addr = addr;
                                info!("rejoin with new leader at {}", leader_addr);
                                continue;
                            }
                            ResultCode::NoLeader => {
                                // let peers: Vec<String> = deserialize(&response.data)?;
                                info!("no leader, continue ask peers after 10 seconds",);
                                tokio::time::sleep(Duration::from_secs(10)).await;
                                continue;
                            }
                            ResultCode::Ok => {
                                if change.change_type == ConfChangeType::AddNode as i32 {
                                    info!("join successfully with leader at {}", leader_addr);
                                    // return Ok(node);
                                    return Ok(handle);
                                } else if change.change_type == ConfChangeType::RemoveNode as i32 {
                                    // removed node, add node again
                                    change.set_change_type(ConfChangeType::AddNode);
                                    continue;
                                }
                            }
                            _ => {
                                error!("failed to join using peer {}, try next one", &leader_addr);
                                break;
                            }
                        }
                    }
                    Err(_) => {
                        error!("failed to connect to peer {}, try next one", &leader_addr);
                        break;
                    }
                }
            }
        }

        error!("failed to join cluster with any peers");
        Err(Error::JoinError)
    }
}
