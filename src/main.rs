#![warn(missing_docs)]
//! A simple Raft implementation in Rust

mod config;
mod consensus;
mod messaging;
mod state_machine;

use std::{collections::HashMap, sync::Arc, time::Duration};

use config::Config;
use consensus::{ConsensusError, ConsensusEvent, NodeServer, NodeTimer};
use log::{debug, error, info};
use messaging::{Message, Network, NodeMessenger};
use state_machine::StateMachine;
use tokio::sync::{Mutex, broadcast};

/// Helper to send a command message to a specific node
async fn send_command_to_node(
    nodes_messengers: &HashMap<u64, NodeMessenger>,
    node_id: u64,
    message: Message,
) -> Result<(), ConsensusError> {
    if let Some(messenger) = nodes_messengers.get(&node_id) {
        // Use send_self because the command originates "externally" but targets the
        // node's loop
        messenger.send_self(message).await.map_err(ConsensusError::Transport)
    } else {
        Err(ConsensusError::NodeNotFound(node_id))
    }
}

/// Wait for an event to occur.
async fn wait_for_event(
    rx: &mut broadcast::Receiver<ConsensusEvent>,
    expected_event: ConsensusEvent,
    timeout_duration: Duration,
) -> Result<(), ConsensusError> {
    let timeout = tokio::time::timeout(timeout_duration, rx.recv()).await;
    match timeout {
        Ok(Ok(event)) =>
            if event == expected_event {
                Ok(())
            } else {
                error!("Simulation: Expected event but got {:?}", event);
                Err(ConsensusError::Timeout(
                    "Simulation: Expected event {:?} but got {:?}".to_string(),
                ))
            },
        Ok(Err(e)) => {
            error!("Simulation: Error receiving event: {:?}", e);
            Err(ConsensusError::Timeout("Simulation: Error receiving event".to_string()))
        }
        Err(_) => {
            error!("Simulation: Timeout waiting for event {:?}", expected_event);
            Err(ConsensusError::Timeout("Simulation: Timeout waiting for event {:?}".to_string()))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), ConsensusError> {
    // Initialize logging
    env_logger::init();

    // Create a config
    let config = Config { node_count: 2, ..Default::default() };

    // Create a broadcast channel for consensus events.
    let (event_tx, mut event_rx) = broadcast::channel::<ConsensusEvent>(16);

    let network = Arc::new(Mutex::new(Network::new()));
    let mut nodes: HashMap<u64, Arc<Mutex<NodeServer>>> = HashMap::new();
    let mut nodes_messengers: HashMap<u64, NodeMessenger> = HashMap::new();

    info!("Simulation: Setting up {} nodes...", config.node_count);

    for id in 0..config.node_count as u64 {
        // Create a new node messenger and receiver
        let (node_messenger, mut node_receiver) = NodeMessenger::new(id, network.clone());

        // Add sender to the network
        network.lock().await.add_node(id, node_messenger.sender.clone());

        // Add messenger to the nodes messengers map (to send commands)
        nodes_messengers.insert(id, node_messenger.clone());

        // Create a new timer
        let mut timer = NodeTimer::new(config.clone());

        // Create a new node
        let node_server =
            NodeServer::new(id, StateMachine::new(), node_messenger, event_tx.clone());
        let node_server_arc = Arc::new(Mutex::new(node_server));
        // Store the node in the nodes map
        nodes.insert(id, node_server_arc.clone());

        // Spawn a new task to process incoming messages for the node
        tokio::spawn(async move {
            // Get the node id
            let node_server_id = { node_server_arc.lock().await.id() };

            info!("Simulation: Start processing messages for node {}", node_server_id);

            loop {
                // Acquire lock for processing single message
                let mut node_locked = node_server_arc.lock().await;
                // let msg = node_locked.receive_message().await;

                tokio::select! {
                  msg = node_receiver.receive() => {
                    match msg {
                      Ok(msg) => {
                        debug!("Simulation: Node {} received message: {:?}", node_server_id, msg.clone());
                        // Process message
                        let step_result = node_locked.process_message(msg.clone(), &mut timer).await;

                        match step_result {
                          Ok(()) => {
                            debug!("Simulation: Node {} finished step successfully.", node_server_id);
                          }
                          Err(e) => {
                            error!(
                              "!!! Simulation: Error processing message {:?} for node {}: {:?}",
                              msg, node_server_id, e
                            );
                          }
                        }
                      }
                      Err(e) => {
                        error!(
                            "!!! Simulation: Error receiving message for node {}: {:?}",
                            node_server_id, e
                        );
                        break;
                      }
                    }
                  }

                  // Wrap lock acquisition and await in an async block
                  timer_event = timer.wait_for_timer_and_emit_event() => {
                    debug!("Simulation: Node {} timer event triggered: {:?}", node_server_id, timer_event);

                    // This still requires the NodeServer lock
                    let result = node_locked.handle_timer_event(timer_event, &mut timer).await;

                    if let Err(e) = result {
                      error!(
                        "!!! Simulation: Error handling timer event for node {}: {:?}",
                        node_server_id, e
                      );
                      break;
                    }
                  }
                }

                drop(node_locked);
                tokio::task::yield_now().await;
            }
        });
    }

    drop(event_tx);

    info!("Simulation: Nodes initialized, tasks spawned");

    info!("Simulation: Starting election for node 0");
    send_command_to_node(&nodes_messengers, 0, Message::StartElectionCmd).await?;

    // Wait for the LeaderElected event.
    info!("Simulation: Waiting for leader elected event...");
    wait_for_event(
        &mut event_rx,
        ConsensusEvent::LeaderElected { leader_id: 0 },
        Duration::from_secs(10),
    )
    .await?;

    info!("Simulation: Starting append entries...");

    send_command_to_node(
        &nodes_messengers,
        0,
        Message::StartAppendEntriesCmd { command: "Hello, world!".to_string() },
    )
    .await?;

    info!("Simulation: Waiting for entry committed event...");
    wait_for_event(
        &mut event_rx,
        ConsensusEvent::EntryCommitted { entry: "Hello, world!".to_string() },
        Duration::from_secs(10),
    )
    .await?;

    info!("Simulation: Complete");
    info!("Press Ctrl+C to exit.");
    tokio::signal::ctrl_c().await.expect("failed to listen for ctrl+c");

    Ok(())
}
