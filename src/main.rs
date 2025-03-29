mod consensus;
mod messaging;
mod state_machine;
mod utils;
use std::{collections::HashMap, sync::Arc};

use consensus::{ConsensusError, Node, NodeState};
use messaging::{Network, NodeMessenger};
use state_machine::StateMachine;
use tokio::sync::Mutex;

async fn simulate_election(
    nodes: &mut HashMap<u64, Arc<Mutex<Node>>>,
    candidate_id: u64,
) -> Result<(), ConsensusError> {
    // Get candidate
    let candidate_arc =
        nodes.get(&candidate_id).ok_or(ConsensusError::NodeNotFound(candidate_id))?;

    // Update node state (transition to candidate) and then drop the lock
    let mut candidate = candidate_arc.lock().await;
    let election_term = candidate.current_term() + 1;
    candidate.transition_to(NodeState::Candidate, election_term);
    candidate.broadcast_vote_request().await?;

    Ok(())
}

// TODO: update simulation to process messages internally
async fn simulate_append_entries(
    nodes: &mut HashMap<u64, Arc<Mutex<Node>>>,
    leader_id: u64,
) -> Result<(), ConsensusError> {
    // Get leader by temp removing it from the nodes map
    let leader_arc = nodes.get(&leader_id).ok_or(ConsensusError::NodeNotFound(leader_id))?;

    let mut leader = leader_arc.lock().await;
    leader.append_to_log_and_broadcast("command".to_string()).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), ConsensusError> {
    let network = Arc::new(Mutex::new(Network::new()));
    let mut nodes: HashMap<u64, Arc<Mutex<Node>>> = HashMap::new();

    for id in 0..5 {
        let (node_messenger, node_receiver) = NodeMessenger::new(network.clone());
        let node = Node::new(id, StateMachine::new(), node_messenger.clone(), node_receiver);
        let node_arc = Arc::new(Mutex::new(node));
        nodes.insert(id, node_arc.clone());
        network.lock().await.add_node(id, node_messenger);
        tokio::spawn(async move {
            let node_id = node_arc.lock().await.id();
            if let Err(e) = node_arc.lock().await.process_incoming_messages().await {
                eprintln!("Node {} error: {}", node_id, e);
            }
        });
    }

    simulate_election(&mut nodes, 0).await?;

    // simulate_append_entries(&mut nodes, 0).await?;

    Ok(())
}
