use std::sync::Arc;

use log::error;
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
    pub async fn send_to(
        &self,
        from: u64,
        to: u64,
        message: Message,
    ) -> Result<(), MessagingError> {
        let network = self.network.lock().await;
        network.send_message(from, to, message).await.map_err(|e| {
            error!("Error sending message to node {}: {}", to, e);
            MessagingError::SendError(to)
        })
    }

    /// Broadcasts a message to all nodes using the global Network.
    pub async fn broadcast(&self, from: u64, message: Message) -> Result<(), MessagingError> {
        let network = self.network.lock().await;
        network.broadcast(from, message).await.map_err(|e| {
            error!("Error broadcasting message: {}", e);
            MessagingError::BroadcastError
        })
    }

    /// Returns the number of nodes in the network.
    pub async fn get_nodes_count(&self) -> Result<usize, MessagingError> {
        let network = self.network.lock().await;
        Ok(network.get_nodes_count())
    }
}
