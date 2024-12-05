use std::collections::HashMap;

use raft::{
    eraftpb::{ConfChange, Message as RaftMessage},
    StateRole,
};
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
    RaftState { role: RaftRole },
}

/// The role of the node.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default, Serialize, Deserialize)]
pub enum RaftRole {
    /// The node is a follower of the leader.
    #[default]
    Follower,
    /// The node could become a leader.
    Candidate,
    /// The node is a leader.
    Leader,
    /// The node could become a candidate, if `prevote` is enabled.
    PreCandidate,
    /// The node could become a candidate, if `prevote` is enabled.
    Unknown,
}
impl RaftRole {
    pub fn from(role: StateRole) -> Self {
        if role == StateRole::Follower {
            RaftRole::Follower
        } else if role == StateRole::Leader {
            RaftRole::Leader
        } else if role == StateRole::Candidate {
            RaftRole::Candidate
        } else if role == StateRole::PreCandidate {
            RaftRole::PreCandidate
        } else {
            RaftRole::Unknown
        }
    }
}

#[allow(dead_code)]
pub enum Message {
    Leave {
        reply_chan: Sender<RaftResponse>,
    },
    RaftState {
        reply_chan: Sender<RaftResponse>,
    },
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
