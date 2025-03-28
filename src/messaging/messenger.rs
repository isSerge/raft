use std::sync::Arc;

use tokio::sync::{Mutex, mpsc};

use crate::messaging::{Message, MessagingError, Network};

/// A messaging system for a node
#[derive(Debug, Clone)]
pub struct NodeMessenger {
    // TODO: create abstraction for network
    network: Arc<Mutex<Network>>,
    sender: mpsc::Sender<Message>,
    receiver: Arc<Mutex<mpsc::Receiver<Message>>>,
}

impl NodeMessenger {
    pub fn new(network: Arc<Mutex<Network>>) -> Self {
        let (sender, receiver) = mpsc::channel(100);
        Self { network, sender, receiver: Arc::new(Mutex::new(receiver)) }
    }

    // Sends a message directly into this node's own queue.
    pub async fn send(&self, message: Message) -> Result<(), MessagingError> {
        self.sender.send(message).await.map_err(|_| MessagingError::SendError)
    }

    // Receives a message from this node's own queue.
    pub async fn receive(&self) -> Result<Message, MessagingError> {
        let mut receiver = self.receiver.lock().await;
        receiver.try_recv().map_err(|_| MessagingError::ReceiveError)
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
