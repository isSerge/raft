use std::{collections::HashMap, sync::Arc};

use log::info;
use tokio::sync::Mutex;

use crate::consensus::NodeServer;

/// Print the state of all nodes in the network.
pub async fn print_node_state(nodes: &HashMap<u64, Arc<Mutex<NodeServer>>>) {
    for (id, node) in nodes {
        let node = node.lock().await;

        info!(
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
