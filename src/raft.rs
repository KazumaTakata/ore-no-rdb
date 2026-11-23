use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::{DateTime, TimeDelta, Utc};
use tokio::{sync::Mutex, time::sleep};

use crate::cluster::raft::{raft_client::RaftClient, AppendEntriesRequest, RequestVoteRequest};

#[derive(Debug, PartialEq)]
pub enum ConsensusModuleState {
    Follower,
    Candidate,
    Leader,
    Dead,
}

#[derive(Debug, Default)]
struct LogEntry {
    command: String,
    term: u16,
}

#[derive(Debug)]
pub struct ConsensusModule {
    id: i32,
    peer_ids: Vec<i32>,
    pub current_term: u32,
    pub voted_for: Option<i32>,
    log: Vec<LogEntry>,
    pub state: ConsensusModuleState,
    pub election_reset_event: DateTime<Utc>,
}

fn new_consensus_module(id: i32, peer_ids: Vec<i32>) -> Arc<Mutex<ConsensusModule>> {
    let consensus_module = Arc::new(Mutex::new(ConsensusModule {
        id: id,
        peer_ids: peer_ids,
        current_term: 0,
        voted_for: None,
        log: vec![],
        election_reset_event: Utc::now(),
        state: ConsensusModuleState::Follower,
    }));

    let consensus_module_clone = Arc::clone(&consensus_module);

    tokio::spawn(async move {
        {
            let mut consensus_module = consensus_module_clone.lock().await;
            consensus_module.election_reset_event = Utc::now();
        }

        run_election_timer(consensus_module_clone)
    });

    return consensus_module;
}

pub async fn become_follower(consensus_module: Arc<Mutex<ConsensusModule>>, term: u32) {
    let consensus_module_clone = consensus_module.clone();
    let mut consensus_module = consensus_module.lock().await;

    consensus_module.state = ConsensusModuleState::Follower;
    consensus_module.current_term = term;
    consensus_module.voted_for = None;
    consensus_module.election_reset_event = Utc::now();

    tokio::spawn(async move { run_election_timer(consensus_module_clone) });
}

async fn start_leader(consensus_module: Arc<Mutex<ConsensusModule>>) {
    let consensus_module_clone = consensus_module.clone();
    let mut consensus_module = consensus_module.lock().await;

    consensus_module.state = ConsensusModuleState::Leader;

    tokio::spawn(async move { loop {} });
}

async fn leader_send_heatbeats(consensus_module: Arc<Mutex<ConsensusModule>>) {
    let mut consensus_module_value = consensus_module.lock().await;

    if consensus_module_value.state != ConsensusModuleState::Leader {
        return;
    }

    let saved_current_term = consensus_module_value.current_term;

    for peer_id in consensus_module_value.peer_ids.iter() {
        let consensus_module_clone = consensus_module.clone();
        let args = AppendEntriesRequest {
            leader_id: consensus_module_value.id,
            term: saved_current_term,
        };

        tokio::spawn(async move {
            let mut client = RaftClient::connect("http://[::1]:50051").await.unwrap();

            let request = tonic::Request::new(args);

            let response = client.append_entries(request).await.unwrap();

            let term = response.into_inner().term;

            if term > saved_current_term {
                become_follower(consensus_module_clone, term);
                return;
            }
        });
    }
}

async fn run_election_timer(consensus_module: Arc<Mutex<ConsensusModule>>) {
    let timeout_duration = run_election_timeout();
    let term_started = {
        let consensus_module = consensus_module.lock().await;
        consensus_module.current_term
    };
    loop {
        sleep(Duration::from_millis(100)).await;
        let mut consensus_module = consensus_module.lock().await;

        if consensus_module.state != ConsensusModuleState::Candidate
            && consensus_module.state != ConsensusModuleState::Follower
        {
            return;
        }

        if term_started != consensus_module.current_term {
            return;
        }
        let elapsed = Utc::now() - consensus_module.election_reset_event;

        if elapsed >= timeout_duration {
            start_election(consensus_module);
        }
    }
}

async fn start_election(consensus_module: Arc<Mutex<ConsensusModule>>) {
    let mut consensus_module_value = consensus_module.lock().await;
    consensus_module_value.state = ConsensusModuleState::Candidate;
    consensus_module_value.current_term += 1;
    let saved_current_term = consensus_module_value.current_term;
    consensus_module_value.election_reset_event = Utc::now();
    consensus_module_value.voted_for = Some(consensus_module_value.id);

    let votes_received = Arc::new(AtomicUsize::new(1));

    for peer_id in consensus_module_value.peer_ids.iter() {
        let consensus_module_clone = consensus_module.clone();
        let votes_received = votes_received.clone();

        tokio::spawn(async move {
            let consensus_module_value = consensus_module_clone.lock().await;
            let args = RequestVoteRequest {
                term: saved_current_term,
                candidate_id: consensus_module_value.id,
            };
            let mut client = RaftClient::connect("http://[::1]:50051").await.unwrap();
            let request = tonic::Request::new(args);
            let response = client.request_vote(request).await.unwrap();

            let reply = response.into_inner();

            if consensus_module_value.state != ConsensusModuleState::Candidate {
                return;
            }

            if reply.term > saved_current_term {
                become_follower(consensus_module_clone.clone(), reply.term);
                return;
            }

            if reply.term == saved_current_term && reply.vote_granted {
                let votes = votes_received.fetch_add(1, Ordering::SeqCst) + 1;

                if votes * 2 > consensus_module_value.peer_ids.len() + 1 {
                    start_leader(consensus_module_clone.clone());
                    return;
                }
            }
        });
    }

    let consensus_module_copy = consensus_module.clone();
    tokio::spawn(async move { run_election_timer(consensus_module_copy) });
}

fn run_election_timeout() -> TimeDelta {
    let duration = 150 + rand::random_range(0..150);
    TimeDelta::milliseconds(150 + duration)
}
