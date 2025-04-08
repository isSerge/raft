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
                    break leader_id; // Exit loop and return leader_id directly
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
    };

    // Phase 2: Leader appends entries
    let num_commands_to_send = 10;

    info!("Simulation: Sending {} commands to Leader Node {}...", num_commands_to_send, leader_id);

    for i in 1..=num_commands_to_send {
        // Loop to send multiple commands
        let command = format!("Command {}", i);
        debug!("Simulation: Sending command '{}' to Leader {}", command, leader_id);
        send_command_to_node(
            &nodes_messengers,
            leader_id,
            Message::StartAppendEntriesCmd { command },
        )
        .await?;
        // Optional: Small delay between commands to simulate client behavior
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    info!("Simulation: Finished sending commands.");

    // Phase 3: Verify that the command was appended to the leader's log
    let target_last_applied = num_commands_to_send; // We expect index 1 to be applied
    let target_state_value = num_commands_to_send; // Assuming state machine increments by 1
    let verification_start_time = tokio::time::Instant::now();
    let mut success = false; // Assume failure until proven otherwise
    let verification_timeout = Duration::from_secs(10);

    info!(
        "Simulation: Verifying application of commit index {} across {} nodes (timeout: {:?})...",
        target_last_applied, config.node_count, verification_timeout
    );

    loop {
        // Check timeout first
        if verification_start_time.elapsed() > verification_timeout {
            error!(
                "Simulation: Verification timeout after {:?}. Target state not reached on all \
                 nodes.",
                verification_timeout
            );
            break;
        }

        // Assume this iteration will succeed unless a node fails the check
        let mut all_nodes_ok_this_iteration = true;

        for id in 0..config.node_count as u64 {
            let node_arc = nodes.get(&id).expect("Node ID should exist");
            let node_locked = node_arc.lock().await; // Lock briefly

            // Check if THIS node meets the condition
            if !(node_locked.last_applied() >= target_last_applied
                && node_locked.state_machine_state() == target_state_value)
            {
                // If any node fails, this iteration is not successful, no need to check others
                all_nodes_ok_this_iteration = false;
                debug!(
                    "Node {} has not reached target state yet (last_applied={}, sm_state={})",
                    id,
                    node_locked.last_applied(),
                    node_locked.state_machine_state()
                );
                break;
            }
        }

        // If we finished the inner loop and all nodes were ok
        if all_nodes_ok_this_iteration {
            success = true;
            info!(
                "Simulation: Verification successful! All {} nodes reached target state \
                 (last_applied >= {}, sm_state={}).",
                config.node_count, target_last_applied, target_state_value
            );
            break;
        }

        // Wait before polling again
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    if !success {
        // Use the success flag determined by the loop
        error!("Simulation: Verification FAILED.");
        // Print final states for debugging
        for id in 0..config.node_count as u64 {
            let node_arc = nodes.get(&id).unwrap();
            let node_locked = node_arc.lock().await;
            error!(
                " -> Final State Node {}: Term={}, State={:?}, CommitIdx={}, LastApplied={}, \
                 SMState={}",
                id,
                node_locked.current_term(),
                node_locked.state(),
                node_locked.commit_index(),
                node_locked.last_applied(),
                node_locked.state_machine_state()
            );
        }
        return Err(ConsensusError::Timeout("State verification failed".to_string()));
    }

    Ok(())
}
