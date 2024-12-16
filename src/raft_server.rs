use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use crate::message::{Message, Response};
use crate::raft_service::raft_service_server::{RaftService, RaftServiceServer};
use crate::raft_service::{Proposal, RaftServiceResponse, ResultCode as RaftServiceResultCode};
use crate::ServiceProposalRequest;

use bincode::serialize;
use log::{debug, error, info};
use raft::eraftpb::Message as RaftMessage;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tonic::transport::Server;
use tonic::{Request, Response as GrpcResponse, Status};

pub struct RaftServer {
    raft_node_tx: mpsc::Sender<Message>,
    proposal_service_tx: mpsc::Sender<ServiceProposalRequest>,
    addr: SocketAddr,
}

impl RaftServer {
    pub fn new<A: ToSocketAddrs>(
        raft_node_tx: mpsc::Sender<Message>,
        leader_service_tx: mpsc::Sender<ServiceProposalRequest>,
        addr: A,
    ) -> Self {
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        RaftServer {
            raft_node_tx,
            proposal_service_tx: leader_service_tx,
            addr,
        }
    }

    pub async fn run(self) {
        let addr = self.addr;
        info!("listening gRPC requests on: {}", addr);
        let svc = RaftServiceServer::new(self);
        Server::builder()
            .add_service(svc)
            .serve(addr)
            .await
            .expect("error running raft grpc server");
        panic!("raft grpc server has quit");
    }
}

#[tonic::async_trait]
impl RaftService for RaftServer {
    /// use it to send AddNode or RemoveNode ConfigChange to leader, if node is not leader, return WrongLeader response with leader addr
    async fn change_config(
        &self,
        req: Request<raft::eraftpb::ConfChange>,
    ) -> Result<GrpcResponse<RaftServiceResponse>, Status> {
        info!("received change config requests from: {:?}", &req);
        let change = req.into_inner();
        let raft_node_tx = self.raft_node_tx.clone();

        let (tx, rx) = oneshot::channel();
        let message = Message::ConfigChange {
            change: change,
            reply_chan: tx,
        };

        match raft_node_tx.send(message).await {
            Ok(_) => (),
            Err(_) => error!("failed to send config change to local node via tx channel"),
        }

        let mut default_reply = RaftServiceResponse::default();

        // if we don't receive a response after 2secs, we timeout
        let response = timeout(Duration::from_secs(2), rx).await;
        info!("change config response = {:?}", response);
        match response {
            Ok(Ok(res)) => match res {
                Response::WrongLeader { leader_addr } => {
                    return Ok(GrpcResponse::new(RaftServiceResponse {
                        code: RaftServiceResultCode::WrongLeader as i32,
                        data: serialize(&(leader_addr)).unwrap(),
                    }));
                }
                Response::NoLeader { peer_addrs } => {
                    return Ok(GrpcResponse::new(RaftServiceResponse {
                        code: RaftServiceResultCode::NoLeader as i32,
                        data: serialize(&(peer_addrs)).unwrap(),
                    }));
                }
                Response::JoinSuccess { peer_addrs } => {
                    return Ok(GrpcResponse::new(RaftServiceResponse {
                        code: RaftServiceResultCode::Ok as i32,
                        data: serialize(&(peer_addrs)).unwrap(),
                    }));
                }
                Response::Ok => (),
                r => {
                    default_reply.code = RaftServiceResultCode::Error as i32;
                    default_reply.data = serialize(&r).unwrap();
                    error!("unexpected response {:?}", &r);
                }
            },
            Ok(Err(_)) => {
                default_reply.code = RaftServiceResultCode::Error as i32;
                default_reply.data = serialize(&Response::Error).unwrap();
                error!("timeout waiting for response");
            }
            Err(_e) => {
                default_reply.code = RaftServiceResultCode::Error as i32;
                default_reply.data = serialize(&Response::Error).unwrap();
                error!("timeout waiting for response");
            }
        }

        Ok(GrpcResponse::new(default_reply))
    }

    /// send a raft internal messages between nodes, e.g. heartbeat, election campaign, proposal.
    async fn send_raft_message(
        &self,
        request: Request<RaftMessage>,
    ) -> Result<GrpcResponse<RaftServiceResponse>, Status> {
        let message = request.into_inner();
        // again this ugly shit to serialize the message
        // debug!("send message {:?}", message);
        let raft_node_tx = self.raft_node_tx.clone();
        match raft_node_tx.send(Message::Raft(Box::new(message))).await {
            Ok(_) => (),
            Err(_) => error!("failed to send raft message to local node via tx channel"),
        }

        Ok(GrpcResponse::new(RaftServiceResponse::default()))
    }

    /// send a request to a internal service registered to let service determine and send proposals
    /// into raft MailBox to broadcast to other nodes.
    /// usually only leader node will make proposal, service should check if it's leader.
    async fn request_service_proposal(
        &self,
        request: Request<Proposal>,
    ) -> Result<GrpcResponse<RaftServiceResponse>, Status> {
        let _ = self
            .proposal_service_tx
            .send(ServiceProposalRequest {
                inner: request.into_inner().inner,
            })
            .await;
        Ok(GrpcResponse::new(RaftServiceResponse::default()))
    }
}
