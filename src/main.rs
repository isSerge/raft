mod consensus;
mod messaging;
mod state_machine;
mod utils;
use std::{collections::HashMap, sync::Arc, time::Duration};

use consensus::{ConsensusError, Node};
use log::{error, info};
use messaging::{Message, Network, NodeMessenger};
use state_machine::StateMachine;
use tokio::sync::Mutex;

// Helper to send a command message to a specific node
async fn send_command_to_node(
    nodes_messengers: &HashMap<u64, NodeMessenger>, // Pass messengers map
    node_id: u64,
    message: Message,
) -> Result<(), ConsensusError> {
    if let Some(messenger) = nodes_messengers.get(&node_id) {
        // Use send_self because the command originates "externally" but targets the
        // node's loop
        let msg_arc = Arc::new(message);
        messenger.send_self(msg_arc).await.map_err(ConsensusError::Transport)
    } else {
        Err(ConsensusError::NodeNotFound(node_id))
    }
}

#[tokio::main]
async fn main() -> Result<(), ConsensusError> {
    // Initialize logging (e.g., using env_logger)
    env_logger::init();

    let network = Arc::new(Mutex::new(Network::new()));
    let mut nodes: HashMap<u64, Arc<Mutex<Node>>> = HashMap::new();
    let mut nodes_messengers: HashMap<u64, NodeMessenger> = HashMap::new();

    let node_count = 5;
    info!("Setting up {} nodes...", node_count);

    for id in 0..node_count {
        // Create a new node messenger and receiver
        let (node_messenger, node_receiver) = NodeMessenger::new(id, network.clone());

        // Add sender to the network
        network.lock().await.add_node(id, node_messenger.sender.clone());

        // Add messenger to the nodes messengers map (to send commands)
        nodes_messengers.insert(id, node_messenger.clone());

        // Create a new node
        let node = Node::new(id, StateMachine::new(), node_messenger, node_receiver);
        let node_arc = Arc::new(Mutex::new(node));
        // Store the node in the nodes map
        nodes.insert(id, node_arc.clone());

        // Spawn a new task to process incoming messages for the node
        tokio::spawn(async move {
            let mut node_locked = node_arc.lock().await;
            info!("Start processing messages for node {}", node_locked.id());

            if let Err(e) = node_locked.process_incoming_messages().await {
                error!("Node {} error: {}", node_locked.id(), e);
            }
        });
    }

    info!("Nodes initialized, tasks spawned");

    info!("Starting election for node 0");
    send_command_to_node(&nodes_messengers, 0, Message::StartElectionCmd).await?;

    // Give time for election to potentially happen
    info!("Waiting for election process...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    Ok(())
}
