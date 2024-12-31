use crate::error::Result;
use crate::message::{Message, Response};
use crate::message::{RaftChange, RaftRole};
use crate::raft::Store;
use crate::raft_service::raft_service_client::RaftServiceClient;
use crate::raft_service::Proposal;
use crate::storage::{LogStore, MemStorage};
use bincode::{deserialize, serialize};
use log::{debug, error, info, warn};
use prost::Message as PMessage;
use raft::eraftpb::{ConfChange, ConfChangeType, Entry, EntryType, Message as RaftMessage};
use raft::{prelude::*, raw_node::RawNode, Config};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tonic::transport::channel::Channel;
use tonic::transport::Endpoint;
use tonic::Request;

struct MessageSender {
    message: RaftMessage,
    client: RaftServiceClient<tonic::transport::channel::Channel>,
    client_id: String,
    reply_tx: mpsc::Sender<Message>,
    max_retries: usize,
    timeout: Duration,
}

impl MessageSender {
    /// attempt to send a message MessageSender::max_retries times at MessageSender::timeout
    /// inteval.
    async fn send(mut self) {
        let mut current_retry = 0usize;
        loop {
            let message_request = Request::new(self.message.clone());
            match self.client.send_raft_message(message_request).await {
                Ok(_) => {
                    return;
                }
                Err(e) => {
                    if current_retry < self.max_retries {
                        current_retry += 1;
                        tokio::time::sleep(self.timeout).await;
                    } else {
                        error!(
                            "failed to send raft message {:?} to {} after {} retries: {}",
                            self.message, self.client_id, current_retry, e
                        );
                        let _ = self
                            .reply_tx
                            .send(Message::ReportUnreachable {
                                node_id: self.client_id,
                            })
                            .await;
                        return;
                    }
                }
            }
        }
    }
}

pub struct PeerStatus {
    unreachable_cnt: u8,
}
impl PeerStatus {
    pub fn inc_get_unreachble_cnt(&mut self) -> u8 {
        self.unreachable_cnt += 1;
        self.unreachable_cnt
    }
}

pub struct Peer {
    addr: String,
    client: RaftServiceClient<Channel>,
}

impl Deref for Peer {
    type Target = RaftServiceClient<Channel>;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for Peer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

impl Peer {
    pub async fn new(addr: &str) -> Result<Peer> {
        // TODO: clean up this mess
        debug!("connecting to node at {}...", addr);
        let ep = Endpoint::from_str(&format!("http://{}", addr))
            .unwrap()
            .timeout(Duration::from_secs(2))
            .connect_timeout(Duration::from_secs(2));
        let client = RaftServiceClient::connect(ep).await?;
        debug!("connected to node at {}.", addr);
        Ok(Peer {
            addr: addr.to_string(),
            client,
        })
    }
}

pub struct RaftNode<S: Store> {
    inner: RawNode<MemStorage>,
    pub peers: HashMap<String, Option<Peer>>,
    pub peers_status: HashMap<String, PeerStatus>,
    pub rcv: mpsc::Receiver<Message>,
    pub snd: mpsc::Sender<Message>,
    store: S,
    should_quit: bool,
    leader_quiting: bool,
    seq: AtomicU64,
    last_snap_time: Instant,
}

impl<S: Store + 'static + Send> RaftNode<S> {
    pub fn new_leader(
        id: &str,
        rcv: mpsc::Receiver<Message>,
        snd: mpsc::Sender<Message>,
        store: S,
        logger: Option<&slog::Logger>,
    ) -> Self {
        let config = Config {
            id: id.to_string(),
            election_tick: 100,
            // Heartbeat tick is for how long the leader needs to send
            // a heartbeat to keep alive.
            heartbeat_tick: 10,
            // Just for log
            ..Default::default()
        };

        config.validate().unwrap();

        let mut s = Snapshot::default();
        // Because we don't use the same configuration to initialize every node, so we use
        // a non-zero index to force new followers catch up logs by snapshot first, which will
        // bring all nodes to the same initial state.
        s.mut_metadata().index = 1;
        s.mut_metadata().term = 1;
        s.mut_metadata().mut_conf_state().voters = vec![id.to_string()];

        let mut storage = MemStorage::create();
        storage.apply_snapshot(s).unwrap();
        let mut inner = if logger.is_some() {
            RawNode::new(&config, storage, logger.unwrap()).unwrap()
        } else {
            RawNode::with_default_logger(&config, storage).unwrap()
        };
        let peers = HashMap::new();
        let peers_status = HashMap::new();
        let seq = AtomicU64::new(0);
        let last_snap_time = Instant::now();

        inner.raft.become_candidate();
        inner.raft.become_leader();

        RaftNode {
            inner,
            rcv,
            peers,
            peers_status,
            store,
            seq,
            snd,
            should_quit: false,
            leader_quiting: false,
            last_snap_time,
        }
    }

    pub fn new_follower(
        id: &str,
        rcv: mpsc::Receiver<Message>,
        snd: mpsc::Sender<Message>,
        store: S,
        logger: Option<&slog::Logger>,
    ) -> Result<Self> {
        let config = Config {
            id: id.to_string(),
            election_tick: 100,
            // Heartbeat tick is for how long the leader needs to send
            // a heartbeat to keep alive.
            heartbeat_tick: 10,
            // Just for log
            ..Default::default()
        };

        config.validate().unwrap();

        let mut s = Snapshot::default();
        // // Because we don't use the same configuration to initialize every node, so we use
        // // a non-zero index to force new followers catch up logs by snapshot first, which will
        // // bring all nodes to the same initial state.
        s.mut_metadata().index = 1;
        s.mut_metadata().term = 1;
        s.mut_metadata().mut_conf_state().voters = vec![id.to_string()];
        let mut storage = MemStorage::create();
        storage.apply_snapshot(s).unwrap();

        let storage = MemStorage::create();
        let inner = if logger.is_some() {
            RawNode::new(&config, storage, logger.unwrap()).unwrap()
        } else {
            RawNode::with_default_logger(&config, storage).unwrap()
        };
        let peers = HashMap::new();
        let peers_status = HashMap::new();
        let seq = AtomicU64::new(0);
        let last_snap_time = Instant::now()
            .checked_sub(Duration::from_secs(1000))
            .unwrap();

        Ok(RaftNode {
            inner,
            rcv,
            peers,
            peers_status,
            store,
            seq,
            snd,
            should_quit: false,
            leader_quiting: false,
            last_snap_time,
        })
    }

    pub fn add_peer_status(&mut self, addr: &str) -> &mut PeerStatus {
        let p = PeerStatus { unreachable_cnt: 0 };
        self.peers_status.insert(addr.to_string(), p);
        self.peers_status.get_mut(addr).unwrap()
    }

    pub fn peer_status_mut(&mut self, addr: &str) -> Option<&mut PeerStatus> {
        match self.peers_status.get_mut(addr) {
            None => None,
            Some(v) => Some(v),
        }
    }

    pub fn remove_peer_status(&mut self, addr: &str) {
        self.peers_status.remove(addr);
    }

    pub fn peer_mut(&mut self, id: &str) -> Option<&mut Peer> {
        match self.peers.get_mut(id) {
            None => None,
            Some(v) => v.as_mut(),
        }
    }

    pub fn is_leader(&self) -> bool {
        self.inner.raft.leader_id == self.inner.raft.id
    }

    pub fn id_mut(&mut self) -> &str {
        &self.raft.id
    }

    pub fn id(&self) -> &str {
        &self.raft.id
    }

    pub async fn add_peer(&mut self, addr: &str) -> Result<()> {
        let peer = Peer::new(addr).await?;
        self.peers.insert(addr.to_string(), Some(peer));
        Ok(())
    }

    fn leader(&self) -> &str {
        &self.raft.leader_id
    }

    fn peer_addrs(&self) -> Vec<String> {
        self.peers
            .iter()
            .filter_map(|(_, peer)| peer.as_ref().map(|Peer { addr, .. }| (addr.to_string())))
            .collect()
    }

    fn send_wrong_leader(&self, channel: oneshot::Sender<Response>) {
        let leader_id = self.leader();
        info!("not a leader, send my leader info {}", &leader_id);
        // leader can't be an empty node
        if let Some(Some(leader)) = self.peers.get(leader_id) {
            info!("not a leader, send my leader info {}", &leader_id);
            let leader_addr = leader.addr.clone();
            let raft_response = Response::WrongLeader { leader_addr };
            // TODO handle error here
            let _ = channel.send(raft_response);
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let mut heartbeat = Duration::from_millis(100);
        let mut now = Instant::now();
        let mut leader_quit_tick = 0;
        let mut quit_tick = 0;

        // A map to contain sender to client responses
        let mut client_send = HashMap::new();

        loop {
            if self.should_quit {
                let quit_tick_times = if self.leader_quiting { 100 } else { 10 };
                if quit_tick > quit_tick_times {
                    warn!("Quitting raft");
                    return Ok(());
                }
                quit_tick += 1
            }
            match timeout(heartbeat, self.rcv.recv()).await {
                Ok(Some(Message::ConfigChange {
                    reply_chan,
                    mut change,
                })) => {
                    info!(
                        "as {:?}, received config change: {:?}",
                        self.inner.raft.state, change,
                    );
                    // whenever a change id is "0", it's a message to self.
                    if change.get_node_id() == "" {
                        change.set_node_id(self.id().to_string());
                    }

                    if self.leader() == "" {
                        let raft_response = Response::NoLeader {
                            peer_addrs: self.peer_addrs(),
                        };
                        let _ = reply_chan.send(raft_response);
                    } else if !self.is_leader() {
                        self.send_wrong_leader(reply_chan);
                    } else {
                        if change.change_type == ConfChangeType::RemoveNode as i32 {
                            self.propose_remove_node(change.get_node_id()).await?;
                            let _ = reply_chan.send(Response::Ok);
                        } else {
                            self.propose_add_node(change.get_node_id()).await?;
                            let _ = reply_chan.send(Response::Ok);
                        }
                    }
                }
                Ok(Some(Message::Raft(m))) => {
                    debug!(
                        "as {:?}, step raft message: {:?}, {:?}, ",
                        &self.raft.state,
                        &m,
                        &self.inner.snap(),
                    );
                    if let Ok(_a) = self.step(*m) {};
                }
                Ok(Some(Message::Leave { reply_chan })) => {
                    info!("as {:?}, received leave message", &self.raft.state,);
                    let node_id = self.id().to_string();
                    self.propose_remove_node(&node_id).await?;
                    let _ = reply_chan.send(Response::Ok);
                }
                Ok(Some(Message::ServiceProposalRequest { request })) => {
                    info!(
                        "as {:?}, received request service proposal message",
                        &self.raft.state,
                    );
                    let leader_addr = self.leader().to_string();
                    if leader_addr != "" {
                        if let Some(p) = self.peer_mut(&leader_addr) {
                            let _ = p
                                .client
                                .request_service_proposal(Request::new(Proposal { inner: request }))
                                .await;
                        } else {
                            // should send message to itself if it's leader
                            if let Ok(mut peer) = Peer::new(&leader_addr).await {
                                let _ = peer
                                    .client
                                    .request_service_proposal(Request::new(Proposal {
                                        inner: request,
                                    }))
                                    .await;
                            }
                        }
                    } else {
                        info!("leader is unknown, can not send service proposal message");
                    }
                }
                Ok(Some(Message::RaftState { reply_chan })) => {
                    debug!("as {:?}, received role state message", &self.raft.state);
                    let raft_response = Response::RaftState {
                        role: RaftRole::from(self.raft.state),
                        leader_id: self.leader().to_string(),
                    };
                    let _ = reply_chan.send(raft_response);
                }
                Ok(Some(Message::Propose {
                    proposal,
                    reply_chan,
                })) => {
                    debug!(
                        "as {:?}, received propose message: {:?}",
                        &self.raft.state, &proposal
                    );
                    let seq = self.seq.fetch_add(1, Ordering::Relaxed);
                    client_send.insert(seq, reply_chan);
                    let seq = serialize(&seq).unwrap();
                    let _ = self.propose(seq, proposal);
                }
                Ok(Some(Message::ReportUnreachable { node_id })) => {
                    info!(
                        "as {:?}, found node {} is not reachable",
                        &self.raft.state, &node_id
                    );
                    self.report_unreachable(node_id.clone());
                    let peer = match self.peer_status_mut(&node_id) {
                        Some(v) => v,
                        None => self.add_peer_status(&node_id),
                    };
                    let cnt = peer.inc_get_unreachble_cnt();
                    if cnt > 10 {
                        let _ = self.propose_remove_node(&node_id).await;
                        self.remove_peer_status(&node_id)
                    }
                }
                Ok(_) => unreachable!(),
                Err(_) => (),
            }

            let elapsed = now.elapsed();
            now = Instant::now();
            if elapsed > heartbeat {
                heartbeat = Duration::from_millis(100);
                self.tick();
                // if leader, give some time to cleanup, some external proposal need to happen
                if self.leader_quiting {
                    leader_quit_tick += 1;
                    // slow heartbeat of leader when it's quiting
                    heartbeat = Duration::from_millis(500);
                    warn!("Leader is quitting, ticking {}", leader_quit_tick);
                    if leader_quit_tick > 20 {
                        // ugly borrow check, use empty string, will fill value inside
                        let _ = self.propose_remove_node_config_change("").await;
                    }
                }
            } else {
                heartbeat -= elapsed;
            }

            self.on_ready(&mut client_send).await?;
        }
    }

    async fn on_ready(
        &mut self,
        client_send: &mut HashMap<u64, oneshot::Sender<Response>>,
    ) -> Result<()> {
        if !self.has_ready() {
            return Ok(());
        }

        let mut ready = self.ready();

        if !ready.messages().is_empty() {
            // Send out the messages.
            self.send_messages(ready.take_messages()).await;
        }
        if *ready.snapshot() != Snapshot::default() {
            let snapshot = ready.snapshot();
            info!("update snapshot {:?}", snapshot);
            self.store.restore(snapshot.get_data()).await?;
            let store = self.mut_store();
            store.apply_snapshot(snapshot.clone())?;
        }

        self.handle_committed_entries(ready.take_committed_entries(), client_send)
            .await?;

        if !ready.entries().is_empty() {
            let entries = &ready.entries()[..];
            let store = self.mut_store();
            store.append(entries)?;
        }

        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            let store = self.mut_store();
            store.set_hard_state(hs)?;
        }

        if !ready.persisted_messages().is_empty() {
            // Send out the persisted messages come from the node.
            self.send_messages(ready.take_persisted_messages()).await;
        }

        let mut light_rd = self.advance(ready);

        if let Some(commit) = light_rd.commit_index() {
            let store = self.mut_store();
            store.set_hard_state_comit(commit)?;
        }

        // Send out the messages.
        self.send_messages(light_rd.take_messages()).await;

        // Apply all committed entries.
        self.handle_committed_entries(light_rd.take_committed_entries(), client_send)
            .await?;

        self.advance_apply();

        Ok(())
    }

    async fn send_messages(&mut self, msgs: Vec<RaftMessage>) {
        for msg in msgs {
            let to_addr = msg.get_to();
            let client = match self.peer_mut(to_addr) {
                Some(ref peer) => peer.client.clone(),
                None => match self.add_peer(to_addr).await {
                    Ok(_) => self.peer_mut(to_addr).unwrap().client.clone(),
                    Err(_) => {
                        let _ = self
                            .snd
                            .send(Message::ReportUnreachable {
                                node_id: to_addr.to_string(),
                            })
                            .await;
                        continue;
                    }
                },
            };
            let message_sender = MessageSender {
                client_id: to_addr.to_string(),
                client: client,
                reply_tx: self.snd.clone(),
                message: msg,
                timeout: Duration::from_millis(100),
                max_retries: 2,
            };
            tokio::task::spawn(message_sender.send());
        }
    }

    async fn handle_committed_entries(
        &mut self,
        committed_entries: Vec<Entry>,
        client_send: &mut HashMap<u64, oneshot::Sender<Response>>,
    ) -> Result<()> {
        // Fitler out empty entries produced by new elected leaders.
        for entry in committed_entries {
            if entry.get_data().is_empty() {
                // Emtpy entry, when the peer becomes Leader it will send an empty entry.
                continue;
            }
            if let EntryType::EntryConfChange = entry.get_entry_type() {
                self.handle_config_change(&entry, client_send).await?;
            } else {
                self.handle_normal(&entry, client_send).await?;
            }
        }
        Ok(())
    }

    async fn handle_config_change(
        &mut self,
        entry: &Entry,
        senders: &mut HashMap<u64, oneshot::Sender<Response>>,
    ) -> Result<()> {
        let seq: u64 = deserialize(entry.get_context())?;
        let change: ConfChange = PMessage::decode(entry.get_data())?;
        let change_type = change.get_change_type();

        match change_type {
            ConfChangeType::AddNode => {
                let addr: String = deserialize(change.get_context())?;
                info!("adding node {} to peers", addr);
                let _ = self.add_peer(&addr).await;
            }
            ConfChangeType::RemoveNode => {
                if change.get_node_id() == self.id() {
                    self.should_quit = true;
                    warn!("node {} quiting the cluster", &self.id());
                } else {
                    warn!("removing peer {} from cluster", change.get_node_id());
                    self.peers.remove(change.get_node_id());
                }
            }
            _ => unimplemented!(),
        }

        if let Ok(cs) = self.apply_conf_change(&change) {
            let last_applied = self.raft.raft_log.applied;
            let snapshot = self.store.snapshot().await?;
            {
                let store = self.mut_store();
                store.set_conf_state(&cs)?;
                store.compact(last_applied)?;
                store.create_snapshot(snapshot)?;
            }
        }

        if let Some(sender) = senders.remove(&seq) {
            let response = match change_type {
                ConfChangeType::AddNode => Response::JoinSuccess {
                    peer_addrs: self.peer_addrs(),
                },
                ConfChangeType::RemoveNode => Response::Ok,
                _ => unimplemented!(),
            };
            if sender.send(response).is_err() {
                error!("error sending back config change response")
            }
        }
        Ok(())
    }

    async fn handle_normal(
        &mut self,
        entry: &Entry,
        senders: &mut HashMap<u64, oneshot::Sender<Response>>,
    ) -> Result<()> {
        let seq: u64 = deserialize(entry.get_context())?;
        let data = self.store.apply(entry.get_data()).await?;
        if let Some(sender) = senders.remove(&seq) {
            let _ = sender.send(Response::Response { data });
        }

        if Instant::now() > self.last_snap_time + Duration::from_secs(15) {
            info!("creating backup..");
            self.last_snap_time = Instant::now();
            let last_applied = self.raft.raft_log.applied;
            let snapshot = self.store.snapshot().await?;
            let store = self.mut_store();
            if store.compact(last_applied).is_ok() {
                let _ = store.create_snapshot(snapshot);
            }
        }
        Ok(())
    }

    async fn propose_remove_node(&mut self, node_id: &str) -> Result<()> {
        info!("propose removing node {}", node_id);
        let _ = self
            .snd
            .clone()
            .send(Message::ServiceProposalRequest {
                request: serialize(&RaftChange::RemoveNode {
                    node_id: node_id.to_string(),
                })
                .unwrap(),
            })
            .await;

        if self.is_leader() && self.raft.id == node_id {
            tokio::time::sleep(Duration::from_secs(2)).await;
            // leader use two phase exit, it give it some graceful period to handle service
            // proposal and send config change after graceful period
            self.leader_quiting = true;
            Ok(())
        } else {
            let r = self.propose_remove_node_config_change(node_id).await;
            // force follower to quit even propose failed, leader detect disconnected node and resend proposal
            if self.raft.id == node_id {
                self.should_quit = true;
            }
            r
        }
    }

    async fn propose_remove_node_config_change(&mut self, node_id: &str) -> Result<()> {
        let id = if node_id == "" {
            self.id().to_string()
        } else {
            node_id.to_string()
        };
        info!("propose removing node conf change {}", id);
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let mut change = ConfChange::default();
        change.set_node_id(id.clone());
        change.set_change_type(ConfChangeType::RemoveNode);
        change.set_context(serialize(&self.id())?);
        let ret = match self.propose_conf_change(serialize(&seq).unwrap(), change) {
            Ok(_) => Ok(()),
            Err(e) => {
                error!(
                    "failed to propose removing node conf change {}, error: {:?}",
                    id.as_str(),
                    e
                );
                Err(crate::Error::Unknown)
            }
        };
        ret
    }

    async fn propose_add_node(&mut self, node_id: &str) -> Result<()> {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let mut change = ConfChange::default();
        change.set_node_id(node_id.to_string());
        change.set_change_type(ConfChangeType::AddNode);
        change.set_context(serialize(&self.id())?);
        let ret = match self.propose_conf_change(serialize(&seq).unwrap(), change) {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("failed to propose adding node {}, error: {:?}", node_id, e);
                Err(crate::Error::Unknown)
            }
        };
        ret
    }
}

impl<S: Store> Deref for RaftNode<S> {
    type Target = RawNode<MemStorage>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S: Store> DerefMut for RaftNode<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
