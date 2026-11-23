use std::sync::Arc;

use chrono::Utc;
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};

use crate::{
    cluster::raft::{
        raft_server::{Raft, RaftServer},
        RequestVoteReply, RequestVoteRequest,
    },
    raft::{become_follower, ConsensusModule, ConsensusModuleState},
};

pub mod raft {
    tonic::include_proto!("raft");
}
#[derive(Debug)]
pub struct RaftService {
    consensus_module: Arc<Mutex<ConsensusModule>>,
}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        println!("Got a request: {:?}", request);

        let consensus_module = self.consensus_module.lock().await;

        if consensus_module.state == ConsensusModuleState::Dead {
            return Err(Status::new(tonic::Code::FailedPrecondition, "".to_string()));
        }

        let term = request.into_inner().term;
        let candidate_id = request.into_inner().candidate_id;

        if request.into_inner().term > consensus_module.current_term {
            become_follower(self.consensus_module.clone(), term);
        }

        if consensus_module.current_term == term
            && (consensus_module.voted_for == None
                || consensus_module.voted_for == Some(candidate_id))
        {
            consensus_module.voted_for = Some(candidate_id);
            consensus_module.election_reset_event = Utc::now();
        }

        return Ok(RequestVoteReply {});
    }
}

#[tokio::main]
pub async fn handle_cluster() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;

    let raft_service = RaftService {};

    Server::builder()
        .add_service(RaftServer::new(raft_service))
        .serve(addr)
        .await?;

    Ok(())
}

#[tokio::main]
pub async fn handle_cluster_test_clinet() -> Result<(), Box<dyn std::error::Error>> {
    //
    //     let mut client = GreeterClient::connect("http://[::1]:50051").await?;
    //
    //     let request = tonic::Request::new(HelloRequest {
    //         name: "Tonic".into(),
    //     });
    //
    //     let response = client.say_hello(request).await?;
    //
    //     println!("RESPONSE={:?}", response);
    //

    Ok(())
}
