use std::collections::HashMap;

use crate::messaging::{Message, MessagingError, NodeMessenger};

/// A network of nodes
#[derive(Debug)]
pub struct Network {
    nodes: HashMap<u64, NodeMessenger>,
}

impl Network {
    pub fn new() -> Self {
        Self { nodes: HashMap::new() }
    }

    pub fn add_node(&mut self, node_id: u64, node_messenger: NodeMessenger) {
        self.nodes.insert(node_id, node_messenger);
    }

    /// Send a message to a specific node
    pub fn send_message(&self, from: u64, to: u64, message: Message) -> Result<(), MessagingError> {
        if let Some(dest) = self.nodes.get(&to) {
            println!("Routing message from node {} to node {}", from, to);
            dest.send(message).map_err(|_| MessagingError::SendError)
        } else {
            eprintln!("Destination node {} not found", to);
            Err(MessagingError::NodeNotFound(to))
        }
    }

    /// Broadcast a message to all nodes
    pub fn broadcast(&self, from: u64, message: Message) -> Result<(), MessagingError> {
        for (node_id, node_messenger) in &self.nodes {
            // Don't send message to itself
            if *node_id != from {
                println!("Broadcasting message from node {} to node {}", from, node_id);
                node_messenger.send(message.clone()).map_err(|_| MessagingError::SendError)?;
            }
        }
        Ok(())
    }
}
