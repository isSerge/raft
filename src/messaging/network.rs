use std::collections::HashMap;

use log::{error, info, warn};
use tokio::sync::mpsc;

use crate::messaging::{Message, MessagingError};

/// A network of nodes
#[derive(Debug)]
pub struct Network {
    node_senders: HashMap<u64, mpsc::Sender<Message>>,
}

impl Network {
    pub fn new() -> Self {
        Self { node_senders: HashMap::new() }
    }

    /// Add a node to the network
    pub fn add_node(&mut self, node_id: u64, node_sender: mpsc::Sender<Message>) {
        info!("Adding Node {} to network", node_id);
        self.node_senders.insert(node_id, node_sender);
    }

    /// Send a message to a specific node
    pub async fn send_message(
        &self,
        from: u64,
        to: u64,
        message: Message,
    ) -> Result<(), MessagingError> {
        if let Some(dest_sender) = self.node_senders.get(&to) {
            info!("Routing message from node {} to node {}", from, to);
            dest_sender.send(message).await.map_err(|e| {
                error!("Error sending message to node {}: {}", to, e);
                MessagingError::SendError(to)
            })
        } else {
            warn!("Destination node {} not found", to);
            Err(MessagingError::NodeNotFound(to))
        }
    }

    /// Broadcast a message to all nodes
    pub async fn broadcast(&self, from: u64, message: Message) -> Result<(), MessagingError> {
        for (node_id, node_sender) in &self.node_senders {
            // Don't send message to itself
            if *node_id != from {
                info!("Broadcasting message from node {} to node {}", from, node_id);
                node_sender.send(message.clone()).await.map_err(|e| {
                    error!("Error sending message to node {}: {}", node_id, e);
                    MessagingError::SendError(*node_id)
                })?;
            }
        }
        Ok(())
    }

    /// Returns the number of nodes in the network.
    pub fn get_nodes_count(&self) -> usize {
        self.node_senders.len()
    }
}
