use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    consensus::{Node, NodeState},
    messaging::{Network, NodeMessenger},
    state_machine::StateMachine,
};

fn create_node(id: u64, network: Arc<Mutex<Network>>) -> Node {
    Node::new(id, StateMachine::new(), NodeMessenger::new(network))
}

#[test]
fn test_node_transition_to_candidate_and_vote_for_self() {
    const NODE_ID: u64 = 1;
    const TERM: u64 = 1;
    let network = Arc::new(Mutex::new(Network::new()));
    let mut node = create_node(NODE_ID, network.clone());

    node.transition_to(NodeState::Candidate, TERM);

    assert_eq!(node.state(), NodeState::Candidate);
    assert_eq!(node.current_term(), TERM);
    assert_eq!(node.voted_for(), Some(NODE_ID));
}

#[test]
fn test_node_transition_to_follower_and_reset_voted_for() {
    const NODE_ID: u64 = 1;
    const TERM: u64 = 1;
    let network = Arc::new(Mutex::new(Network::new()));
    let mut node = create_node(NODE_ID, network.clone());

    assert_eq!(node.state(), NodeState::Follower);
    assert_eq!(node.current_term(), 0);
    assert_eq!(node.voted_for(), None);

    node.transition_to(NodeState::Candidate, TERM);

    assert_eq!(node.state(), NodeState::Candidate);
    assert_eq!(node.current_term(), TERM);
    assert_eq!(node.voted_for(), Some(NODE_ID));

    const NEW_TERM: u64 = TERM + 1; // Increment the term to trigger a transition to follower

    node.transition_to(NodeState::Follower, NEW_TERM);

    assert_eq!(node.state(), NodeState::Follower);
    assert_eq!(node.current_term(), NEW_TERM);
    assert_eq!(node.voted_for(), None);
}

#[test]
fn test_node_transition_to_leader() {
    const NODE_ID: u64 = 1;
    const TERM: u64 = 1;
    let network = Arc::new(Mutex::new(Network::new()));
    let mut node = create_node(NODE_ID, network.clone());

    assert_eq!(node.state(), NodeState::Follower);
    assert_eq!(node.current_term(), 0);
    assert_eq!(node.voted_for(), None);

    node.transition_to(NodeState::Leader, TERM);

    assert_eq!(node.state(), NodeState::Leader);
    assert_eq!(node.current_term(), TERM);
    assert_eq!(node.voted_for(), Some(NODE_ID));
}
