mod consensus;
mod messaging;
mod state_machine;

use std::sync::{Arc, Mutex};

use consensus::Node;
use messaging::Network;
use state_machine::StateMachine;

fn main() {
    let network = Arc::new(Mutex::new(Network::new()));

    for id in 0..5 {
        let node = Node::new(id, StateMachine::new());
        network.lock().unwrap().add_node(node);
    }
}
