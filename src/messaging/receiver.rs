use std::sync::Arc;

use tokio::sync::mpsc;

use crate::messaging::{Message, MessagingError};

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
