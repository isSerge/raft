use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    consensus::{ConsensusError, Node, NodeState},
    messaging::{Message, Network, NodeMessenger, NodeReceiver},
    state_machine::StateMachine,
};

fn create_node(id: u64, node_messenger: NodeMessenger, node_receiver: NodeReceiver) -> Node {
    Node::new(id, StateMachine::new(), node_messenger, node_receiver)
}

#[test]
fn test_node_transition_to_candidate_and_vote_for_self() {
    const NODE_ID: u64 = 1;
    const TERM: u64 = 1;
    let network = Arc::new(Mutex::new(Network::new()));
    let (node_messenger, node_receiver) = NodeMessenger::new(network.clone());
    let mut node = create_node(NODE_ID, node_messenger, node_receiver);

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
    let (node_messenger, node_receiver) = NodeMessenger::new(network.clone());
    let mut node = create_node(NODE_ID, node_messenger, node_receiver);

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
    let (node_messenger, node_receiver) = NodeMessenger::new(network.clone());
    let mut node = create_node(NODE_ID, node_messenger, node_receiver);

    assert_eq!(node.state(), NodeState::Follower);
    assert_eq!(node.current_term(), 0);
    assert_eq!(node.voted_for(), None);

    node.transition_to(NodeState::Leader, TERM);

    assert_eq!(node.state(), NodeState::Leader);
    assert_eq!(node.current_term(), TERM);
    assert_eq!(node.voted_for(), Some(NODE_ID));
}

#[tokio::test]
async fn test_node_broadcast_vote_request_fails_if_not_candidate() {
    const NODE_ID: u64 = 1;
    let network = Arc::new(Mutex::new(Network::new()));
    let (node_messenger, node_receiver) = NodeMessenger::new(network.clone());
    let node = create_node(NODE_ID, node_messenger, node_receiver);

    assert_eq!(node.state(), NodeState::Follower);

    let result = node.broadcast_vote_request().await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), ConsensusError::NotCandidate(NODE_ID));
}

#[tokio::test]
async fn test_node_broadcast_vote_request_sends_message_to_all_nodes() {
    const NODE_ID_1: u64 = 1;
    const NODE_ID_2: u64 = 2;
    const TERM: u64 = 1;
    let network = Arc::new(Mutex::new(Network::new()));
    let (node_messenger_1, node_receiver_1) = NodeMessenger::new(network.clone());
    let (node_messenger_2, node_receiver_2) = NodeMessenger::new(network.clone());
    let mut node_1 = create_node(NODE_ID_1, node_messenger_1.clone(), node_receiver_1);
    let mut node_2 = create_node(NODE_ID_2, node_messenger_2.clone(), node_receiver_2);

    network.lock().await.add_node(NODE_ID_1, node_messenger_1);
    network.lock().await.add_node(NODE_ID_2, node_messenger_2);

    node_1.transition_to(NodeState::Candidate, TERM);

    let result = node_1.broadcast_vote_request().await;
    assert!(result.is_ok());

    let message = node_2.receive_message().await;

    if let Ok(Message::VoteRequest { term, candidate_id }) = message {
        assert_eq!(term, TERM);
        assert_eq!(candidate_id, NODE_ID_1);
    } else {
        panic!("Expected a VoteRequest message");
    }
}
