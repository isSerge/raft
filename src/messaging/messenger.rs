use std::sync::Arc;

use log::{debug, error};
use tokio::sync::{Mutex, mpsc};

use crate::messaging::{Message, MessagingError, Network};

/// A messaging system for a node
#[derive(Debug, Clone)]
pub struct NodeMessenger {
    id: u64,
    // TODO: create abstraction for network
    network: Arc<Mutex<Network>>,
    pub sender: mpsc::Sender<Message>,
}

/// A receiver for messages from this node's own queue.
#[derive(Debug)]
pub struct NodeReceiver {
    id: u64,
    receiver: mpsc::Receiver<Message>,
}

impl NodeReceiver {
    pub fn new(id: u64, receiver: mpsc::Receiver<Message>) -> Self {
        Self { id, receiver }
    }

    /// Receives a message from this node's own queue.
    pub async fn receive(&mut self) -> Result<Message, MessagingError> {
        match self.receiver.recv().await {
            Some(message) => Ok(message),
            None => Err(MessagingError::ReceiveError(self.id)),
        }
    }
}

impl NodeMessenger {
    pub fn new(id: u64, network: Arc<Mutex<Network>>) -> (Self, NodeReceiver) {
        let (sender, receiver) = mpsc::channel(100);
        (Self { id, network, sender }, NodeReceiver::new(id, receiver))
    }

    /// Sends a message to a specific node using the global Network.
    pub async fn send_to(&self, to: u64, message: Message) -> Result<(), MessagingError> {
        debug!("Node {} sending message to {}: {:?}", self.id, to, message);
        self.send_via_network(to, message).await
    }

    /// Returns the number of nodes in the network.
    pub async fn get_nodes_count(&self) -> Result<usize, MessagingError> {
        let network = self.network.lock().await;
        Ok(network.get_nodes_count())
    }

    /// Sends a message directly into this node's *own* queue (e.g., for
    /// commands). Uses the node's own ID as the sender.
    pub async fn send_self(&self, message: Message) -> Result<(), MessagingError> {
        self.sender.send(message).await.map_err(|e| {
            error!("Node {} failed to send message to self: {}", self.id, e);
            MessagingError::SendError(self.id)
        })
    }

    /// Sends a message intended for another node's queue via the Network.
    /// Internal helper used by send_to and broadcast.
    async fn send_via_network(&self, to: u64, message: Message) -> Result<(), MessagingError> {
        let network = self.network.lock().await;
        network.route_message(self.id, to, message).await
    }

    /// Broadcasts a message to all *other* nodes using the global Network.
    pub async fn broadcast(&self, message: Message) -> Result<(), MessagingError> {
        debug!("Node {} broadcasting message: {:?}", self.id, message);
        // Get all node IDs from the network
        let network_locked = self.network.lock().await;
        let all_node_ids: Vec<u64> = network_locked.get_all_node_ids();
        // Drop the lock before sending messages
        drop(network_locked);

        let mut errors = Vec::new();
        for node_id in all_node_ids {
            if node_id != self.id {
                // Don't broadcast to self
                if let Err(e) = self.send_via_network(node_id, message.clone()).await {
                    error!("Node {} failed to broadcast to node {}: {:?}", self.id, node_id, e);
                    // Collect errors
                    errors.push(e);
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            // Return a generic broadcast error if any send failed
            Err(MessagingError::BroadcastError)
        }
    }
}
