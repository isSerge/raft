use std::collections::HashMap;

use log::{debug, error, info, warn};
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

    /// Routes a message from a sender node to a destination node's queue.
    pub async fn route_message(
        &self,
        from: u64,
        to: u64,
        message: Message,
    ) -> Result<(), MessagingError> {
        if let Some(dest_sender) = self.node_senders.get(&to) {
            debug!("Network routing message from {} to {}: {:?}", from, to, message);
            dest_sender.send(message).await.map_err(|e| {
                error!("Network failed to route message from {} to {}: {}", from, to, e);
                MessagingError::SendError(to)
            })
        } else {
            warn!("Network: Destination node {} not found for message from {}", to, from);
            Err(MessagingError::NodeNotFound(to))
        }
    }

    /// Returns the number of nodes in the network.
    pub fn get_nodes_count(&self) -> usize {
        self.node_senders.len()
    }

    /// Returns all node IDs in the network.
    pub fn get_all_node_ids(&self) -> Vec<u64> {
        self.node_senders.keys().cloned().collect()
    }
}
