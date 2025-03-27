use std::collections::HashMap;

use crate::consensus::Node;

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
pub struct Network {
    nodes: HashMap<u64, Node>,
}

impl Network {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, node: Node) {
        self.nodes.insert(node.id, node);
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
