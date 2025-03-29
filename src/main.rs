mod consensus;
mod messaging;
mod state_machine;
mod utils;
use std::{collections::HashMap, sync::Arc};

use consensus::{ConsensusError, Node, NodeState};
use messaging::{Message, Network, NodeMessenger};
use state_machine::StateMachine;
use tokio::sync::Mutex;
use utils::print_node_state;

async fn simulate_election(
    nodes: &mut HashMap<u64, Node>,
    candidate_id: u64,
) -> Result<(), ConsensusError> {
    // Get nodes count first to avoid mutable borrow issues
    let nodes_count = nodes.len();

    // Get candidate by temp removing it from the nodes map
    let mut candidate =
        nodes.remove(&candidate_id).ok_or(ConsensusError::NodeNotFound(candidate_id))?;

    // Get other nodes
    let others: Vec<&mut Node> = nodes.values_mut().collect();

    // 1. Transition to candidate state and broadcast vote request
    if candidate.state() != NodeState::Follower {
        // Reinsert candidate back before returning error.
        nodes.insert(candidate_id, candidate);
        return Err(ConsensusError::NotFollower(candidate_id));
    }

    let election_term = candidate.current_term() + 1;
    candidate.transition_to(NodeState::Candidate, election_term);
    candidate.broadcast_vote_request().await?;

    // 2. Let other nodes vote for the candidate
    for node in others {
        if let Ok(Message::VoteRequest { term, candidate_id }) = node.receive_message().await {
            node.handle_request_vote(term, candidate_id).await?;
        }
    }

    // 3. Wait for responses from other nodes
    // Currently wait for all other nodes to vote
    // TODO: handle votes asynchronously
    let mut votes_received = 1; // self-vote
    for _ in 0..nodes_count - 1 {
        if let Ok(Message::VoteResponse { term: _, vote_granted }) =
            candidate.receive_message().await
        {
            if vote_granted {
                votes_received += 1;
            }
        }
    }

    // 5. Handle election outcome
    if votes_received >= nodes_count - 1 {
        candidate.transition_to(NodeState::Leader, election_term);
        println!(
            "Node {} elected leader with {}/{} votes",
            candidate_id, votes_received, nodes_count
        );
    } else {
        candidate.transition_to(NodeState::Follower, election_term);
        println!(
            "Node {} failed election ({}/{} votes)",
            candidate_id, votes_received, nodes_count
        );
    }

    // Reinsert the candidate back into the map.
    nodes.insert(candidate_id, candidate);

    print_node_state(nodes);
    Ok(())
}

async fn simulate_append_entries(
    nodes: &mut HashMap<u64, Node>,
    leader_id: u64,
) -> Result<(), ConsensusError> {
    // Get leader by temp removing it from the nodes map
    let mut leader = nodes.remove(&leader_id).ok_or(ConsensusError::NodeNotFound(leader_id))?;

    // Get other nodes
    let others: Vec<&mut Node> = nodes.values_mut().collect();

    // Leader appends a new command to its own log and broadcasts it to all other
    // nodes.
    leader.append_to_log_and_broadcast("command".to_string()).await?;

    // Process append entries
    for node in others {
        if let Ok(Message::AppendEntries { term, leader_id, new_entries, commit_index }) =
            node.receive_message().await
        {
            node.handle_append_entries(term, leader_id, &new_entries, commit_index).await?;
        }
    }

    // Reinsert the leader back into the map.
    nodes.insert(leader_id, leader);

    print_node_state(nodes);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), ConsensusError> {
    let network = Arc::new(Mutex::new(Network::new()));
    let mut nodes: HashMap<u64, Node> = HashMap::new();

    for id in 0..5 {
        let (node_messenger, node_receiver) = NodeMessenger::new(network.clone());
        let node = Node::new(id, StateMachine::new(), node_messenger.clone(), node_receiver);
        nodes.insert(id, node);
        network.lock().await.add_node(id, node_messenger);
    }

    simulate_election(&mut nodes, 0).await?;

    simulate_append_entries(&mut nodes, 0).await?;

    Ok(())
}
