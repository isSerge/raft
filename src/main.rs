mod consensus;
mod messaging;
mod state_machine;
mod utils;
use std::sync::Arc;

use consensus::{ConsensusError, Node, NodeState};
use messaging::{Message, Network, NodeMessenger};
use state_machine::StateMachine;
use tokio::sync::Mutex;
use utils::{partition_nodes_mut, print_node_state};

async fn simulate_election(nodes: &mut [Node], candidate_id: u64) -> Result<(), ConsensusError> {
    // Get nodes count first to avoid mutable borrow issues
    let nodes_count = nodes.len();

    // Get candidate and other nodes
    let (candidate, others) = partition_nodes_mut(nodes, candidate_id)?;

    // 1. Transition to candidate state and broadcast vote request
    if candidate.state() != NodeState::Follower {
        return Err(ConsensusError::NotFollower(candidate_id));
    }
    let election_term = candidate.current_term() + 1;
    candidate.transition_to(NodeState::Candidate, election_term);
    candidate.broadcast_vote_request().await?;

    // 2. Calculate majority needed
    let majority_needed = nodes_count / 2 + 1;
    let mut votes_received = 1; // self-vote

    // 3. Process other nodes
    for node in others {
        if let Ok(Message::VoteRequest { term, candidate_id }) = node.receive_message().await {
            node.handle_request_vote(term, candidate_id).await?;
            if node.voted_for() == Some(candidate_id) {
                votes_received += 1;
            }
        }
    }

    // 4. Handle election outcome
    if votes_received >= majority_needed {
        candidate.transition_to(NodeState::Leader, election_term);
        println!(
            "Node {} elected leader with {}/{} votes",
            candidate_id, votes_received, majority_needed
        );
    } else {
        candidate.transition_to(NodeState::Follower, election_term);
        println!(
            "Node {} failed election ({}/{} votes)",
            candidate_id, votes_received, majority_needed
        );
    }

    print_node_state(nodes);
    Ok(())
}

async fn simulate_append_entries(nodes: &mut [Node], leader_id: u64) -> Result<(), ConsensusError> {
    let (leader, others) = partition_nodes_mut(nodes, leader_id)?;

    // Leader appends a new command to its own log and broadcasts it to all other
    // nodes.
    leader.append_to_log_and_broadcast("command".to_string()).await?;

    // Process append entries
    for node in others {
        if let Ok(Message::AppendEntries { term, leader_id, new_entries }) =
            node.receive_message().await
        {
            node.handle_append_entries(term, leader_id, &new_entries).await?;
        }
    }

    print_node_state(nodes);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), ConsensusError> {
    let network = Arc::new(Mutex::new(Network::new()));
    let mut nodes: Vec<Node> = Vec::new();

    for id in 0..5 {
        let node_messenger = NodeMessenger::new(network.clone());
        let node = Node::new(id, StateMachine::new(), node_messenger.clone());
        nodes.push(node);
        network.lock().await.add_node(id, node_messenger);
    }

    simulate_election(&mut nodes, 0).await?;

    simulate_append_entries(&mut nodes, 0).await?;

    Ok(())
}
