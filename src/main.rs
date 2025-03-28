mod consensus;
mod messaging;
mod state_machine;

use std::sync::{Arc, Mutex};

use consensus::{ConsensusError, Node, NodeState};
use messaging::{Message, Network, NodeMessenger};
use state_machine::StateMachine;

fn print_node_state(nodes: &[Node]) {
    for node in nodes {
        println!(
            "Node {}: state: {:?}, term: {}, voted_for: {:?}, log: {:?}, state_machine: {:?}",
            node.id(),
            node.state(),
            node.current_term(),
            node.voted_for(),
            node.log(),
            node.state_machine.get_state()
        );
    }
}

fn simulate_election(nodes: &mut [Node], candidate_id: u64) -> Result<(), ConsensusError> {
    // 1. Find candidate position
    let candidate_idx = nodes
        .iter()
        .position(|n| n.id() == candidate_id)
        .ok_or(ConsensusError::NodeNotFound(candidate_id))?;

    // 2. Process candidate in isolated scope
    let election_term = {
        let candidate = &mut nodes[candidate_idx];
        if candidate.state() != NodeState::Follower {
            return Err(ConsensusError::NotFollower(candidate_id));
        }
        let term = candidate.current_term() + 1;
        candidate.transition_to(NodeState::Candidate, term);
        candidate.broadcast_vote_request()?;
        term
    };

    // 3. Calculate majority needed
    let majority_needed = nodes.len() / 2 + 1;
    let mut votes_received = 1; // self-vote

    // 4. Process other nodes
    let (left, right) = nodes.split_at_mut(candidate_idx);
    let (_, right) = right.split_first_mut().unwrap(); // Skip candidate
    let others = left.iter_mut().chain(right.iter_mut());

    for node in others {
        if let Ok(Message::VoteRequest { term, candidate_id }) = node.receive_message() {
            node.handle_request_vote(term, candidate_id)?;
            if node.voted_for() == Some(candidate_id) {
                votes_received += 1;
            }
        }
    }

    // 5. Handle election outcome
    let candidate = &mut nodes[candidate_idx];
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

fn simulate_append_entries(nodes: &mut [Node], leader_id: u64) -> Result<(), ConsensusError> {
    let leader_idx = nodes
        .iter()
        .position(|n| n.id() == leader_id)
        .ok_or(ConsensusError::NodeNotFound(leader_id))?;

    // Leader appends a new command to its own log and broadcasts it to all other
    // nodes.
    let leader = &mut nodes[leader_idx];
    leader.append_to_log_and_broadcast("command".to_string())?;

    // Process append entries
    for node in nodes.iter_mut().filter(|n| n.id() != leader_id) {
        match node.receive_message() {
            Ok(Message::AppendEntries { term, leader_id, new_entries }) => {
                node.handle_append_entries(term, leader_id, new_entries)?;
            }
            Err(e) => {
                println!("Node {} received error: {:?}", node.id(), e);
            }
            _ => {} // Ignore other message types
        }
    }

    print_node_state(nodes);

    Ok(())
}

fn main() -> Result<(), ConsensusError> {
    let network = Arc::new(Mutex::new(Network::new()));
    let mut nodes: Vec<Node> = Vec::new();

    for id in 0..5 {
        let node_messenger = NodeMessenger::new(network.clone());
        let node = Node::new(id, StateMachine::new(), node_messenger.clone());
        nodes.push(node);
        network.lock().unwrap().add_node_messenger(id, node_messenger);
    }

    simulate_election(&mut nodes, 0)?;

    simulate_append_entries(&mut nodes, 0)?;

    Ok(())
}
