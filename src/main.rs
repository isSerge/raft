mod consensus;
mod messaging;
mod state_machine;

use std::sync::{Arc, Mutex};

use consensus::{Node, NodeState};
use messaging::{Message, Network, NodeMessenger};
use state_machine::StateMachine;

fn simulate_election(
    network: &Arc<Mutex<Network>>,
    nodes: &mut [Node],
    candidate_id: u64,
    candidate_term: u64,
) {
    // Candidate votes for itself before broadcasting the VoteRequest.
    {
        let candidate = nodes.iter_mut().find(|n| n.id == candidate_id).unwrap();
        candidate.current_term = candidate_term;
        candidate.voted_for = Some(candidate_id);
        candidate.state = NodeState::Candidate;
    }

    // Candidate broadcasts a VoteRequest to all other nodes.
    {
        let network = network.lock().unwrap();
        // Broadcast vote request to all nodes
        network
            .broadcast(candidate_id, Message::VoteRequest { term: candidate_term, candidate_id });
    }

    // Simulate each node receiving the vote request and responding.
    for node in nodes.iter_mut() {
        if node.id == candidate_id {
            continue;
        }

        // Receive vote request
        let msg = node.messenger.receive();

        if let Message::VoteRequest { term, candidate_id } = msg {
            node.handle_request_vote(term, candidate_id);
        }
    }

    // After processing the vote requests, print the state of each node.
    for node in &mut *nodes {
        println!(
            "Node {}: state: {:?}, term: {}, voted_for: {:?}",
            node.id, node.state, node.current_term, node.voted_for
        );
    }

    // Check if the candidate won the election and update the state of the
    // candidate.
    let votes = nodes.iter().filter(|n| n.voted_for == Some(candidate_id)).count();
    let has_majority_votes = votes > nodes.len() / 2;
    let candidate_node = nodes.iter_mut().find(|n| n.id == candidate_id).unwrap();

    if has_majority_votes {
        println!("Candidate {} won the election", candidate_id);
        candidate_node.state = NodeState::Leader;
    }

    // Print the state of each node.
    for node in nodes {
        println!(
            "Node {}: state: {:?}, term: {}, voted_for: {:?}",
            node.id, node.state, node.current_term, node.voted_for
        );
    }
}

fn simulate_append_entries(nodes: &mut [Node], leader_id: u64) {
    // Leader appends a new command to its own log.
    {
        let leader = nodes.iter_mut().find(|n| n.id == leader_id).unwrap();
        leader.append_to_log("command".to_string());
    }

    // Each node (except the leader) receives the AppendRequest and responds.
    for node in nodes.iter_mut() {
        if node.id == leader_id {
            continue;
        }

        // Each follower receives the AppendRequest.
        let msg = node.messenger.receive();
        if let Message::AppendEntries { term, leader_id, new_entries } = msg {
            node.handle_append_entries(term, leader_id, new_entries);
        }
    }

    for node in nodes {
        println!(
            "Node {}: state: {:?}, term: {}, state_machine: {:?}, log: {:?}",
            node.id, node.state, node.current_term, node.state_machine.state, node.log
        );
    }
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

    simulate_election(&network, &mut nodes, 0, 1);

    simulate_append_entries(&mut nodes, 0);
}
