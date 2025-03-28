use crate::consensus::{ConsensusError, Node};

pub fn print_node_state(nodes: &[Node]) {
    for node in nodes {
        println!(
            "Node {}: state: {:?}, term: {}, voted_for: {:?}, log: {:?}, state_machine: {:?}",
            node.id(),
            node.state(),
            node.current_term(),
            node.voted_for(),
            node.log(),
            node.state_machine.get_state()
        );
    }
}

/// Splits the nodes slice around the specified node ID, returning:
/// - A mutable reference to the target node
/// - An iterator over all other nodes
pub fn partition_nodes_mut(
    nodes: &mut [Node],
    node_id: u64,
) -> Result<(&mut Node, impl Iterator<Item = &mut Node>), ConsensusError> {
    let idx = nodes
        .iter()
        .position(|n| n.id() == node_id)
        .ok_or(ConsensusError::NodeNotFound(node_id))?;

    let (left, right) = nodes.split_at_mut(idx);
    let (target_node, right) = right.split_first_mut().unwrap();

    let others = left.iter_mut().chain(right.iter_mut());
    Ok((target_node, others))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::Mutex;

    use super::*;
    use crate::{
        consensus::Node,
        messaging::{Network, NodeMessenger},
        state_machine::StateMachine,
    };

    #[tokio::test]
    async fn test_partition_nodes_mut() {
        const NODE_1_IDX: u64 = 0;
        const NODE_2_IDX: u64 = 1;
        const NODE_3_IDX: u64 = 2;
        let network = Arc::new(Mutex::new(Network::new()));
        let (node_1_messenger, node_1_receiver) = NodeMessenger::new(network.clone());
        let node_1 = Node::new(NODE_1_IDX, StateMachine::new(), node_1_messenger, node_1_receiver);
        let (node_2_messenger, node_2_receiver) = NodeMessenger::new(network.clone());
        let node_2 = Node::new(NODE_2_IDX, StateMachine::new(), node_2_messenger, node_2_receiver);
        let (node_3_messenger, node_3_receiver) = NodeMessenger::new(network.clone());
        let node_3 = Node::new(NODE_3_IDX, StateMachine::new(), node_3_messenger, node_3_receiver);

        let mut nodes = vec![node_1, node_2, node_3];
        let (first_node, others) = partition_nodes_mut(&mut nodes, NODE_1_IDX).unwrap();
        assert_eq!(first_node.id(), NODE_1_IDX);
        let others_ids = others.map(|n| n.id()).collect::<Vec<_>>();
        assert!(others_ids.contains(&NODE_2_IDX));
        assert!(others_ids.contains(&NODE_3_IDX));
    }

    #[tokio::test]
    async fn test_partition_nodes_mut_returns_error_if_node_not_found() {
        const NODE_1_IDX: u64 = 0;
        const NODE_2_IDX: u64 = 1;
        const NODE_3_IDX: u64 = 2;
        const NODE_4_IDX: u64 = 3; // non-existent node id
        let network = Arc::new(Mutex::new(Network::new()));
        let (node_1_messenger, node_1_receiver) = NodeMessenger::new(network.clone());
        let node_1 = Node::new(NODE_1_IDX, StateMachine::new(), node_1_messenger, node_1_receiver);
        let (node_2_messenger, node_2_receiver) = NodeMessenger::new(network.clone());
        let node_2 = Node::new(NODE_2_IDX, StateMachine::new(), node_2_messenger, node_2_receiver);
        let (node_3_messenger, node_3_receiver) = NodeMessenger::new(network.clone());
        let node_3 = Node::new(NODE_3_IDX, StateMachine::new(), node_3_messenger, node_3_receiver);

        let mut nodes = vec![node_1, node_2, node_3];
        let result = partition_nodes_mut(&mut nodes, NODE_4_IDX);
        assert!(result.is_err());
    }
}
