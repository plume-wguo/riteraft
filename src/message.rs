use raft::{
    eraftpb::{ConfChange, Message as RaftMessage},
    StateRole,
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot::Sender;

/// copy of raft AddNode and RemoveNode ConfChange, because original struct is not de/serializable
#[derive(Serialize, Deserialize, Debug)]
pub enum RaftChange {
    AddNode { node_id: String },
    RemoveNode { node_id: String },
}

/// copy of raft role. because original struct is not de/serializable
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

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Error,
    JoinSuccess { peer_addrs: Vec<String> },
    NoLeader { peer_addrs: Vec<String> },
    Ok,
    RaftState { role: RaftRole, leader_id: String },
    Response { data: Vec<u8> },
    WrongLeader { leader_addr: String },
}

#[allow(dead_code)]
pub enum Message {
    Leave {
        reply_chan: Sender<Response>,
    },
    ServiceProposalRequest {
        request: Vec<u8>,
        reply_chan: Sender<Response>,
    },
    RaftState {
        reply_chan: Sender<Response>,
    },
    Propose {
        proposal: Vec<u8>,
        reply_chan: Sender<Response>,
    },
    ConfigChange {
        change: ConfChange,
        reply_chan: Sender<Response>,
    },
    ReportUnreachable {
        node_id: String,
    },
    Raft(Box<RaftMessage>),
}
