mod consensus;
mod messaging;
mod state_machine;

use std::sync::{Arc, Mutex};

use consensus::{Node, NodeState};
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

fn simulate_election(nodes: &mut [Node], candidate_id: u64, candidate_term: u64) {
    let candidate = nodes.iter_mut().find(|n| n.id() == candidate_id).unwrap();

    // Verify the candidate is still a follower before starting election
    if candidate.state() != NodeState::Follower {
        println!("Node {} cannot start election from {:?} state", candidate_id, candidate.state());
        return;
    }

    // Candidate votes for itself before broadcasting the VoteRequest.
    candidate.transition_to(NodeState::Candidate, candidate_term);
    // Candidate broadcasts a VoteRequest to all other nodes.
    let result = candidate.broadcast(Message::VoteRequest { term: candidate_term, candidate_id });
    if let Err(e) = result {
        println!("Node {} received error: {:?}", candidate_id, e);
    }

    // Process responses from other nodes
    for node in nodes.iter_mut().filter(|n| n.id() != candidate_id) {
        match node.receive_message() {
            Ok(Message::VoteRequest { term, candidate_id }) => {
                node.handle_request_vote(term, candidate_id);
            }
            Err(e) => {
                println!("Node {} received error: {:?}", node.id(), e);
            }
            _ => {} // Ignore other message types
        }
    }

    print_node_state(nodes);

    // Check for majority
    let votes = nodes.iter().filter(|n| n.voted_for() == Some(candidate_id)).count();
    let majority_needed = nodes.len() / 2 + 1;

    if votes >= majority_needed {
        println!("\nNode {} won election with {}/{} votes", candidate_id, votes, majority_needed);
        if let Some(leader) = nodes.iter_mut().find(|n| n.id() == candidate_id) {
            leader.transition_to(NodeState::Leader, candidate_term);
        }
    } else {
        println!(
            "\nNode {} failed to get majority ({}/{} votes needed)",
            candidate_id, votes, majority_needed
        );

        if let Some(candidate) = nodes.iter_mut().find(|n| n.id() == candidate_id) {
            candidate.transition_to(NodeState::Follower, candidate_term);
        }
    }

    print_node_state(nodes);
}

fn simulate_append_entries(nodes: &mut [Node], leader_id: u64) {
    // Leader appends a new command to its own log.
    {
        let leader = nodes.iter_mut().find(|n| n.id() == leader_id).unwrap();
        leader.append_to_log("command".to_string());
    }

    // Process append entries
    for node in nodes.iter_mut().filter(|n| n.id() != leader_id) {
        match node.receive_message() {
            Ok(Message::AppendEntries { term, leader_id, new_entries }) => {
                node.handle_append_entries(term, leader_id, new_entries);
            }
            Err(e) => {
                println!("Node {} received error: {:?}", node.id(), e);
            }
            _ => {} // Ignore other message types
        }
    }

    print_node_state(nodes);
}

fn main() {
    let network = Arc::new(Mutex::new(Network::new()));
    let mut nodes: Vec<Node> = Vec::new();

    for id in 0..5 {
        let node_messenger = NodeMessenger::new(network.clone());
        let node = Node::new(id, StateMachine::new(), node_messenger.clone());
        nodes.push(node);
        network.lock().unwrap().add_node_messenger(id, node_messenger);
    }

    simulate_election(&mut nodes, 0, 1);

    simulate_append_entries(&mut nodes, 0);
}
