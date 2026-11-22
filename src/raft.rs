use std::{sync::Arc, time::Duration};

use chrono::{DateTime, TimeDelta, Utc};
use tokio::{sync::Mutex, time::sleep};

#[derive(PartialEq)]
enum CMState {
    Follower,
    Candidate,
    Leader,
    Dead,
}

struct LogEntry {
    command: String,
    term: u16,
}

struct ConsensusModule {
    id: i32,
    peer_ids: Vec<i32>,
    current_term: u32,
    voted_for: Option<i32>,
    log: Vec<LogEntry>,
    state: CMState,
    election_reset_event: DateTime<Utc>,
}

fn new_consensus_module(id: i32, peer_ids: Vec<i32>) -> Arc<Mutex<ConsensusModule>> {
    let consensus_module = Arc::new(Mutex::new(ConsensusModule {
        id: id,
        peer_ids: peer_ids,
        current_term: 0,
        voted_for: None,
        log: vec![],
        election_reset_event: Utc::now(),
        state: CMState::Follower,
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

async fn run_election_timer(consensus_module: Arc<Mutex<ConsensusModule>>) {
    let timeout_duration = run_election_timeout();
    let term_started = {
        let consensus_module = consensus_module.lock().await;
        consensus_module.current_term
    };
    loop {
        sleep(Duration::from_millis(100)).await;
        let mut consensus_module = consensus_module.lock().await;

        if consensus_module.state != CMState::Candidate
            && consensus_module.state != CMState::Follower
        {
            return;
        }

        if term_started != consensus_module.current_term {
            return;
        }
        let elapsed = Utc::now() - consensus_module.election_reset_event;

        if elapsed >= timeout_duration {
            start_election(&mut consensus_module);
        }
    }
}

fn start_election(consensus_module: &mut ConsensusModule) {
    consensus_module.state = CMState::Candidate;
    consensus_module.current_term += 1;
    let saved_current_term = consensus_module.current_term;
    consensus_module.election_reset_event = Utc::now();
    consensus_module.voted_for = Some(consensus_module.id);

    let votes_received = 1;

    for peer_id in consensus_module.peer_ids.iter() {
        tokio::spawn(async move {});
    }
}

fn run_election_timeout() -> TimeDelta {
    let duration = 150 + rand::random_range(0..150);
    TimeDelta::milliseconds(150 + duration)
}
