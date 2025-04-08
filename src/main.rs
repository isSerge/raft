#![warn(missing_docs)]
//! A simple Raft implementation in Rust

mod config;
mod consensus;
mod messaging;
mod state_machine;

use std::{collections::HashMap, sync::Arc, time::Duration};

use config::Config;
use consensus::{ConsensusError, ConsensusEvent, NodeServer, NodeTimer};
use log::{debug, error, info, warn};
use messaging::{Message, Network, NodeMessenger};
use state_machine::StateMachineDefault;
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

#[tokio::main]
async fn main() -> Result<(), ConsensusError> {
    // Initialize logging
    env_logger::init();

    // Create a config
    let config = Config { node_count: 3, ..Default::default() };

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
        let node_server = NodeServer::new(
            id,
            Box::new(StateMachineDefault::new()),
            node_messenger,
            event_tx.clone(),
        );
        let node_server_arc = Arc::new(Mutex::new(node_server));
        // Store the node in the nodes map
        nodes.insert(id, node_server_arc.clone());

        // Spawn a new task to process incoming messages for the node
        tokio::spawn(async move {
            // Get the node id
            let node_server_id = { node_server_arc.lock().await.id() };

            info!("Simulation: Start processing messages for node {}", node_server_id);

            loop {
                tokio::select! {
                  msg = node_receiver.receive() => {
                    match msg {
                      Ok(msg) => {
                        debug!("Simulation: Node {} received message: {:?}", node_server_id, msg.clone());
                        // Acquire lock for processing single message
                        let mut node_locked = node_server_arc.lock().await;
                        // Process message
                        let step_result = node_locked.process_message(msg.clone(), &mut timer).await;

                        drop(node_locked);

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
                    // Acquire lock for processing timer event
                    let mut node_locked = node_server_arc.lock().await;
                    // This still requires the NodeServer lock
                    let result = node_locked.handle_timer_event(timer_event, &mut timer).await;

                    drop(node_locked);

                    if let Err(e) = result {
                      error!(
                        "!!! Simulation: Error handling timer event for node {}: {:?}",
                        node_server_id, e
                      );
                      break;
                    }
                  }
                }

                tokio::task::yield_now().await;
            }
        });
    }

    drop(event_tx);

    info!("Simulation: Nodes initialized, tasks spawned");

    info!("Simulation: Waiting for leader elected event...");

    // Phase 1: Leader election
    let leader_id = {
        let mut leader_id_opt = None;
        let election_start_time = tokio::time::Instant::now();
        loop {
            // Check timeout
            if election_start_time.elapsed() > config.election_timeout_max {
                error!(
                    "Simulation: Timeout waiting for leader election after {:?}",
                    config.election_timeout_max
                );
                return Err(ConsensusError::Timeout("Leader election timeout".to_string()));
            }

            // Try receiving an event
            match event_rx.try_recv() {
                Ok(ConsensusEvent::LeaderElected { leader_id }) => {
                    info!("Simulation: Leader Elected: Node {}", leader_id);
                    leader_id_opt = Some(leader_id);
                    break; // Exit loop once leader is found
                }
                Ok(other_event) => {
                    debug!(
                        "Simulation: Ignoring event while waiting for leader: {:?}",
                        other_event
                    );
                }
                Err(broadcast::error::TryRecvError::Empty) => {
                    // No event yet, wait briefly
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(broadcast::error::TryRecvError::Lagged(n)) => {
                    warn!("Simulation: Event receiver lagged by {} messages.", n);
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    error!("Simulation: Event channel closed unexpectedly during leader wait.");
                    return Err(ConsensusError::Timeout("Event channel closed".to_string()));
                }
            }
        }
        leader_id_opt.expect("Loop should not exit without setting leader_id")
    };

    info!("Simulation: Complete");
    info!("Press Ctrl+C to exit.");
    tokio::signal::ctrl_c().await.expect("failed to listen for ctrl+c");

    Ok(())
}
