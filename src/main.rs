mod consensus;
mod messaging;
mod state_machine;

use std::sync::{Arc, Mutex};

use consensus::Node;
use messaging::{Network, NodeMessenger};
use state_machine::StateMachine;

fn main() {
    let network = Arc::new(Mutex::new(Network::new()));
    let mut nodes: Vec<Node> = Vec::new();

    for id in 0..5 {
        let node_messenger = NodeMessenger::new(network.clone());
        let node = Node::new(id, StateMachine::new(), node_messenger.clone());
        nodes.push(node);
        network.lock().unwrap().add_node_messenger(id, node_messenger);
    }
}
