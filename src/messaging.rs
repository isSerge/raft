use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, Sender, channel},
    },
};

use crate::consensus::LogEntry;

#[derive(Debug, thiserror::Error)]
pub enum MessagingError {
    #[error("Destination node {0} not found")]
    NodeNotFound(u64),
    #[error("Failed to send message")]
    SendError,
    #[error("Failed to receive message")]
    ReceiveError,
    #[error("Mutex lock error")]
    MutexError,
    #[error("Failed to broadcast message")]
    BroadcastError,
}

/// A message in the network
#[derive(Debug, Clone)]
pub enum Message {
    /// Request vote from other nodes during election
    VoteRequest { term: u64, candidate_id: u64 },
    /// Response to vote request
    VoteResponse { term: u64, vote_granted: bool },
    /// Request to append entries to other nodes
    AppendEntries { term: u64, leader_id: u64, new_entries: Vec<LogEntry> },
    /// Response to append request
    AppendResponse { term: u64, success: bool },
}

/// A network of nodes
#[derive(Debug)]
pub struct Network {
    nodes: HashMap<u64, NodeMessenger>,
}

impl Network {
    pub fn new() -> Self {
        Self { nodes: HashMap::new() }
    }

    pub fn add_node_messenger(&mut self, node_id: u64, node_messenger: NodeMessenger) {
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
