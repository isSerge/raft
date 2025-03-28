use std::sync::Arc;

use tokio::sync::{Mutex, mpsc};

use crate::messaging::{Message, MessagingError, Network};

/// A messaging system for a node
#[derive(Debug, Clone)]
pub struct NodeMessenger {
    // TODO: create abstraction for network
    network: Arc<Mutex<Network>>,
    sender: mpsc::Sender<Message>,
}

/// A receiver for messages from this node's own queue.
#[derive(Debug)]
pub struct NodeReceiver {
    receiver: mpsc::Receiver<Message>,
}

impl NodeReceiver {
    pub fn new(receiver: mpsc::Receiver<Message>) -> Self {
        Self { receiver }
    }

    /// Receives a message from this node's own queue.
    pub async fn receive(&mut self) -> Result<Message, MessagingError> {
        self.receiver.try_recv().map_err(|_| MessagingError::ReceiveError)
    }
}

impl NodeMessenger {
    pub fn new(network: Arc<Mutex<Network>>) -> (Self, NodeReceiver) {
        let (sender, receiver) = mpsc::channel(100);
        (Self { network, sender }, NodeReceiver::new(receiver))
    }

    // Sends a message directly into this node's own queue.
    pub async fn send(&self, message: Message) -> Result<(), MessagingError> {
        self.sender.send(message).await.map_err(|_| MessagingError::SendError)
    }

    /// Sends a message to a specific node using the global Network.
    pub async fn send_to(
        &self,
        from: u64,
        to: u64,
        message: Message,
    ) -> Result<(), MessagingError> {
        let network = self.network.lock().await;
        network.send_message(from, to, message).await.map_err(|_| MessagingError::SendError)
    }

    /// Broadcasts a message to all nodes using the global Network.
    pub async fn broadcast(&self, from: u64, message: Message) -> Result<(), MessagingError> {
        let network = self.network.lock().await;
        network.broadcast(from, message).await.map_err(|_| MessagingError::BroadcastError)
    }
}
