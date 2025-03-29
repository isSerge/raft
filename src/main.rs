mod consensus;
mod messaging;
mod state_machine;
mod utils;
use std::{collections::HashMap, sync::Arc};

use consensus::{ConsensusError, Node, NodeState};
use messaging::{Network, NodeMessenger};
use state_machine::StateMachine;
use tokio::sync::Mutex;
use utils::print_node_state;

async fn simulate_election(
    nodes: &mut HashMap<u64, Arc<Mutex<Node>>>,
    candidate_id: u64,
) -> Result<(), ConsensusError> {
    // Get candidate by temp removing it from the nodes map
    let candidate_arc =
        nodes.remove(&candidate_id).ok_or(ConsensusError::NodeNotFound(candidate_id))?;

    let election_term; // This is shared between the lock and the rest of the function

    {
        let mut candidate = candidate_arc.lock().await;

        // 1. Transition to candidate state and broadcast vote request
        if candidate.state() != NodeState::Follower {
            // Reinsert candidate back before returning error.
            nodes.insert(candidate_id, candidate_arc.clone());
            return Err(ConsensusError::NotFollower(candidate_id));
        }

        election_term = candidate.current_term() + 1;
        candidate.transition_to(NodeState::Candidate, election_term);
        candidate.broadcast_vote_request().await?;
    } // Lock is released here

    // Reinsert the candidate back into the map.
    nodes.insert(candidate_id, candidate_arc);

    print_node_state(nodes).await;
    Ok(())
}

// TODO: update simulation to process messages internally
// async fn simulate_append_entries(
//     nodes: &mut HashMap<u64, Arc<Mutex<Node>>>,
//     leader_id: u64,
// ) -> Result<(), ConsensusError> {
//     // Get leader by temp removing it from the nodes map
//     let leader_arc =
// nodes.remove(&leader_id).ok_or(ConsensusError::NodeNotFound(leader_id))?;

//     {
//         let mut leader = leader_arc.lock().await;
//         // Leader appends a new command to its own log and broadcasts it to
// all other         // nodes.
//         leader.append_to_log_and_broadcast("command".to_string()).await?;
//     }

//     // Process append entries
//     for node in nodes.values_mut() {
//         let mut node = node.lock().await;
//         if let Ok(Message::AppendEntries { term, leader_id, new_entries,
// commit_index }) =             node.receive_message().await
//         {
//             node.handle_append_entries(term, leader_id, &new_entries,
// commit_index).await?;         }
//     }

//     // Reinsert the leader back into the map.
//     nodes.insert(leader_id, leader_arc);

//     print_node_state(nodes).await;

//     Ok(())
// }

#[tokio::main]
async fn main() -> Result<(), ConsensusError> {
    let network = Arc::new(Mutex::new(Network::new()));
    let mut nodes: HashMap<u64, Arc<Mutex<Node>>> = HashMap::new();

    for id in 0..5 {
        let (node_messenger, node_receiver) = NodeMessenger::new(network.clone());
        let node = Node::new(id, StateMachine::new(), node_messenger.clone(), node_receiver);
        nodes.insert(id, Arc::new(Mutex::new(node)));
        network.lock().await.add_node(id, node_messenger);
    }

    // Start all nodes in parallel
    for (_, node) in nodes.iter() {
        let node_clone = node.clone();
        tokio::spawn(async move {
            node_clone.lock().await.process_incoming_messages().await.unwrap();
        });
    }

    simulate_election(&mut nodes, 0).await?;

    // simulate_append_entries(&mut nodes, 0).await?;

    Ok(())
}
