use std::collections::HashMap;

use raft::eraftpb::{ConfChange, Message as RaftMessage};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot::Sender;

#[derive(Serialize, Deserialize, Debug)]
pub enum RaftResponse {
    NoLeader { peer_addrs: Vec<String> },
    WrongLeader { leader_addr: String },
    JoinSuccess { peer_addrs: Vec<String> },
    Error,
    Response { data: Vec<u8> },
    Ok,
}

#[allow(dead_code)]
pub enum Message {
    Propose {
        proposal: Vec<u8>,
        reply_chan: Sender<RaftResponse>,
    },
    ConfigChange {
        change: ConfChange,
        reply_chan: Sender<RaftResponse>,
    },
    ReportUnreachable {
        node_id: String,
    },
    Raft(Box<RaftMessage>),
}
