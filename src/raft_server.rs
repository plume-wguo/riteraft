use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use crate::message::{Message, RaftResponse};
use crate::raft_service::raft_service_server::{RaftService, RaftServiceServer};
use crate::raft_service::{RaftServiceResponse, ResultCode as RaftServiceResultCode};

use bincode::serialize;
use log::{error, info};
use raft::eraftpb::Message as RaftMessage;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub struct RaftServer {
    snd: mpsc::Sender<Message>,
    addr: SocketAddr,
}

impl RaftServer {
    pub fn new<A: ToSocketAddrs>(snd: mpsc::Sender<Message>, addr: A) -> Self {
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        RaftServer { snd, addr }
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
    async fn change_config(
        &self,
        req: Request<raft::eraftpb::ConfChange>,
    ) -> Result<Response<RaftServiceResponse>, Status> {
        info!("received change config requests from: {:?}", &req);
        let change = req.into_inner();
        let sender = self.snd.clone();

        let (tx, rx) = oneshot::channel();
        let message = Message::ConfigChange {
            change: change,
            chan: tx,
        };

        match sender.send(message).await {
            Ok(_) => (),
            Err(_) => error!("send error"),
        }

        let mut default_reply = RaftServiceResponse::default();

        // if we don't receive a response after 2secs, we timeout
        let response = timeout(Duration::from_secs(2), rx).await;
        info!("change config response = {:?}", response);
        match response {
            Ok(Ok(res)) => match res {
                RaftResponse::WrongLeader { leader_addr } => {
                    return Ok(Response::new(RaftServiceResponse {
                        code: RaftServiceResultCode::WrongLeader as i32,
                        data: serialize(&(leader_addr)).unwrap(),
                    }));
                }
                RaftResponse::JoinSuccess { peer_addrs } => {
                    return Ok(Response::new(RaftServiceResponse {
                        code: RaftServiceResultCode::Ok as i32,
                        data: serialize(&(peer_addrs)).unwrap(),
                    }));
                }
                RaftResponse::Ok => (),
                r => {
                    default_reply.code = RaftServiceResultCode::Error as i32;
                    default_reply.data = serialize(&r).unwrap();
                    error!("unexpected response {:?}", &r);
                }
            },
            Ok(Err(_)) => {
                default_reply.code = RaftServiceResultCode::Error as i32;
                default_reply.data = serialize(&RaftResponse::Error).unwrap();
                error!("timeout waiting for response");
            }
            Err(_e) => {
                default_reply.code = RaftServiceResultCode::Error as i32;
                default_reply.data = serialize(&RaftResponse::Error).unwrap();
                error!("timeout waiting for response");
            }
        }

        Ok(Response::new(default_reply))
    }

    async fn send_message(
        &self,
        request: Request<RaftMessage>,
    ) -> Result<Response<RaftServiceResponse>, Status> {
        let message = request.into_inner();
        // again this ugly shit to serialize the message
        // debug!("send message {:?}", message);
        let sender = self.snd.clone();
        match sender.send(Message::Raft(Box::new(message))).await {
            Ok(_) => (),
            Err(_) => error!("send error"),
        }

        Ok(Response::new(RaftServiceResponse::default()))
    }
}
