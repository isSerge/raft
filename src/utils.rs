use std::collections::HashMap;

use crate::consensus::Node;

pub fn print_node_state(nodes: &HashMap<u64, Node>) {
    for (id, node) in nodes {
        println!(
            "Node {}: state: {:?}, term: {}, voted_for: {:?}, log: {:?}, state_machine: {:?}, \
             commit_index: {}",
            id,
            node.state(),
            node.current_term(),
            node.voted_for(),
            node.log(),
            node.state_machine.get_state(),
            node.commit_index()
        );
    }
}
