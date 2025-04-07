use std::sync::Arc;

use log::{debug, error};
use tokio::sync::{Mutex, mpsc};

use crate::messaging::{Message, MessagingError, Network};

/// A messaging system for a node
#[derive(Debug, Clone)]
pub struct NodeMessenger {
    /// The ID of the node.
    id: u64,
    /// The network of the node.
    // TODO: create abstraction for network
    network: Arc<Mutex<Network>>,
    /// The sender for the node.
    pub sender: mpsc::Sender<Arc<Message>>,
}

/// A receiver for messages from this node's own queue.
#[derive(Debug)]
pub struct NodeReceiver {
    /// The ID of the node.
    id: u64,
    /// The receiver for the node.
    receiver: mpsc::Receiver<Arc<Message>>,
}

impl NodeReceiver {
    pub fn new(id: u64, receiver: mpsc::Receiver<Arc<Message>>) -> Self {
        Self { id, receiver }
    }

    /// Receives a message from this node's own queue.
    pub async fn receive(&mut self) -> Result<Arc<Message>, MessagingError> {
        match self.receiver.recv().await {
            Some(msg_arc) => Ok(msg_arc),
            None => Err(MessagingError::ReceiveError(self.id)),
        }
    }
}

impl NodeMessenger {
    pub fn new(id: u64, network: Arc<Mutex<Network>>) -> (Self, NodeReceiver) {
        let (sender, receiver) = mpsc::channel(100);
        (Self { id, network, sender }, NodeReceiver::new(id, receiver))
    }

    /// Internal helper - takes Arc<Message> and DOES NOT lock network
    async fn send_via_network_unlocked(
        network: &Network, // Takes unlocked network reference
        from_id: u64,
        to: u64,
        msg_arc: Arc<Message>,
    ) -> Result<(), MessagingError> {
        network.route_message(from_id, to, msg_arc).await // Call route_message directly
    }

    /// Sends a message to a specific node using the global Network.
    pub async fn send_to(&self, to: u64, msg_arc: Arc<Message>) -> Result<(), MessagingError> {
        debug!("Node {} sending message to {}: {:?}", self.id, to, msg_arc);
        self.send_via_network(to, msg_arc).await
    }

    /// Returns the number of nodes in the network.
    pub async fn get_nodes_count(&self) -> Result<usize, MessagingError> {
        let network = self.network.lock().await;
        Ok(network.get_nodes_count())
    }

    /// Sends a message directly into this node's *own* queue (e.g., for
    /// commands). Uses the node's own ID as the sender.
    pub async fn send_self(&self, message: Message) -> Result<(), MessagingError> {
        let msg_arc = Arc::new(message);
        self.sender.send(msg_arc).await.map_err(|e| {
            error!("Node {} failed to send message to self: {}", self.id, e);
            MessagingError::SendError(self.id)
        })
    }

    /// Sends a message intended for another node's queue via the Network.
    /// Internal helper used by send_to and broadcast.
    async fn send_via_network(&self, to: u64, msg_arc: Arc<Message>) -> Result<(), MessagingError> {
        let network = self.network.lock().await;
        Self::send_via_network_unlocked(&network, self.id, to, msg_arc).await
    }

    /// Broadcasts a message to all *other* nodes using the global Network.
    pub async fn broadcast(&self, message: Message) -> Result<(), MessagingError> {
        debug!("Node {} broadcasting message: {:?}", self.id, message);
        // Create an Arc for the message to avoid cloning
        let msg_arc = Arc::new(message);
        // Get all node IDs from the network
        let network_locked = self.network.lock().await;
        let all_node_ids: Vec<u64> = network_locked.get_all_node_ids();

        let mut errors = Vec::new();
        for node_id in all_node_ids {
            if node_id != self.id {
                // Clone message Arc for each node
                let msg_arc_clone = Arc::clone(&msg_arc);
                // Don't broadcast to self
                if let Err(e) = Self::send_via_network_unlocked(
                    &network_locked,
                    self.id,
                    node_id,
                    msg_arc_clone,
                )
                .await
                {
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

    /// Returns a slice of all peer IDs in the network.
    pub async fn get_peer_ids(&self) -> Result<Vec<u64>, MessagingError> {
        let network = self.network.lock().await;
        Ok(network.get_all_node_ids())
    }
}
