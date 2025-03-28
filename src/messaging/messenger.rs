use std::sync::{
    Arc, Mutex,
    mpsc::{Receiver, Sender, channel},
};

use crate::messaging::{Message, MessagingError, Network};

/// A messaging system for a node
#[derive(Debug, Clone)]
pub struct NodeMessenger {
    network: Arc<Mutex<Network>>,
    sender: Sender<Message>,
    receiver: Arc<Mutex<Receiver<Message>>>,
}

impl NodeMessenger {
    pub fn new(network: Arc<Mutex<Network>>) -> Self {
        let (sender, receiver) = channel();
        Self { network, sender, receiver: Arc::new(Mutex::new(receiver)) }
    }

    // Sends a message directly into this node's own queue.
    pub fn send(&self, message: Message) -> Result<(), MessagingError> {
        self.sender.send(message).map_err(|_| MessagingError::SendError)
    }

    // Receives a message from this node's own queue.
    pub fn receive(&self) -> Result<Message, MessagingError> {
        let receiver = self.receiver.lock().map_err(|_| MessagingError::MutexError)?;
        receiver.try_recv().map_err(|_| MessagingError::ReceiveError)
    }

    /// Sends a message to a specific node using the global Network.
    pub fn send_to(&self, from: u64, to: u64, message: Message) -> Result<(), MessagingError> {
        let network = self.network.lock().map_err(|_| MessagingError::MutexError)?;
        network.send_message(from, to, message).map_err(|_| MessagingError::SendError)
    }

    /// Broadcasts a message to all nodes using the global Network.
    pub fn broadcast(&self, from: u64, message: Message) -> Result<(), MessagingError> {
        let network = self.network.lock().map_err(|_| MessagingError::MutexError)?;
        network.broadcast(from, message).map_err(|_| MessagingError::BroadcastError)
    }
}
