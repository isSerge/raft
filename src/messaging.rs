use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, Sender, channel},
    },
};

/// A message in the network
#[derive(Debug)]
pub enum Message {
    /// Request vote from other nodes during election
    VoteRequest { term: u64, candidate_id: u64 },
    /// Response to vote request
    VoteResponse { term: u64, vote_granted: bool },
    /// Request to append entries to other nodes
    AppendRequest { term: u64, leader_id: u64 },
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
    pub fn send_message(&self, from: u64, to: u64, message: Message) {
        unimplemented!()
    }

    /// Broadcast a message to all nodes
    pub fn broadcast(&self, message: Message) {
        unimplemented!()
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

    pub fn local_send(&self, message: Message) {
        self.sender.send(message).unwrap();
    }

    pub fn local_receive(&self) -> Message {
        self.receiver.lock().unwrap().recv().unwrap()
    }

    /// Sends a message to a specific node using the global Network.
    pub fn send_to(&self, from: u64, to: u64, message: Message) {
        self.network.lock().unwrap().send_message(from, to, message);
    }
}
