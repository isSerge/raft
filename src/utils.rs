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
