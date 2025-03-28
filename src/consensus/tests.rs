use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    consensus::{ConsensusError, LogEntry, Node, NodeState},
    messaging::{Message, Network, NodeMessenger, NodeReceiver},
    state_machine::StateMachine,
};

/// Create a new node with a given id, messenger, and receiver.
fn create_node(id: u64, node_messenger: NodeMessenger, node_receiver: NodeReceiver) -> Node {
    Node::new(id, StateMachine::new(), node_messenger, node_receiver)
}

/// Create a new network with a given number of nodes.
async fn create_network(number_of_nodes: usize) -> Vec<Node> {
    let network = Arc::new(Mutex::new(Network::new()));
    let mut nodes = Vec::new();
    for i in 0..number_of_nodes {
        let (node_messenger, node_receiver) = NodeMessenger::new(network.clone());
        let node = create_node(i as u64, node_messenger.clone(), node_receiver);
        network.lock().await.add_node(i as u64, node_messenger);
        nodes.push(node);
    }

    nodes
}

#[tokio::test]
async fn test_node_transition_to_candidate_and_vote_for_self() {
    const TERM: u64 = 1;
    const NODE_ID: u64 = 0;

    let mut nodes = create_network(1).await;
    let node = &mut nodes[NODE_ID as usize];

    // check default values
    assert_eq!(node.state(), NodeState::Follower);
    assert_eq!(node.current_term(), 0);
    assert_eq!(node.voted_for(), None);

    node.transition_to(NodeState::Candidate, TERM);

    assert_eq!(node.state(), NodeState::Candidate);
    assert_eq!(node.current_term(), TERM);
    assert_eq!(node.voted_for(), Some(NODE_ID));
}

#[tokio::test]
async fn test_node_transition_to_follower_and_reset_voted_for() {
    const TERM: u64 = 2;
    const NODE_ID: u64 = 0;
    let mut nodes = create_network(1).await;
    let node = &mut nodes[NODE_ID as usize];

    // check default values
    assert_eq!(node.state(), NodeState::Follower);
    assert_eq!(node.current_term(), 0);
    assert_eq!(node.voted_for(), None);

    node.transition_to(NodeState::Candidate, TERM);

    // check values after transition
    assert_eq!(node.state(), NodeState::Candidate);
    assert_eq!(node.current_term(), TERM);
    assert_eq!(node.voted_for(), Some(NODE_ID));

    const NEW_TERM: u64 = TERM + 1; // Increment the term to trigger a transition to follower

    node.transition_to(NodeState::Follower, NEW_TERM);

    assert_eq!(node.state(), NodeState::Follower);
    assert_eq!(node.current_term(), NEW_TERM);
    assert_eq!(node.voted_for(), None);
}

#[tokio::test]
async fn test_node_transition_to_leader() {
    const NODE_ID: u64 = 0;
    const TERM: u64 = 1;
    let mut nodes = create_network(1).await;
    let node = &mut nodes[NODE_ID as usize];

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
    const NODE_ID: u64 = 0;
    let mut nodes = create_network(1).await;
    let node = &mut nodes[NODE_ID as usize];

    assert_eq!(node.state(), NodeState::Follower);

    let result = node.broadcast_vote_request().await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), ConsensusError::NotCandidate(NODE_ID));
}

#[tokio::test]
async fn test_node_broadcast_vote_request_sends_message_to_all_nodes() {
    const NODE_ID_1: u64 = 0;
    const TERM: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    if let [node_1, node_2] = &mut nodes.as_mut_slice() {
        // transition node 1 to candidate
        node_1.transition_to(NodeState::Candidate, TERM);

        // broadcast vote request
        let result = node_1.broadcast_vote_request().await;
        assert!(result.is_ok());

        // receive message from node 2
        let message = node_2.receive_message().await;

        // check that the message is a vote request
        if let Ok(Message::VoteRequest { term, candidate_id }) = message {
            assert_eq!(term, TERM);
            assert_eq!(candidate_id, NODE_ID_1);
        } else {
            panic!("Expected a VoteRequest message");
        }
    } else {
        panic!("Expected 2 nodes");
    }
}

#[tokio::test]
async fn test_node_send_append_response() {
    const TERM: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    if let [node_1, node_2] = &mut nodes.as_mut_slice() {
        // transition node 1 to leader
        node_1.transition_to(NodeState::Leader, TERM);

        // broadcast append entries
        let log_entry = LogEntry::new(TERM, "test".to_string());
        node_1.broadcast_append_entries(vec![log_entry]).await.unwrap();

        // node 2 receives append entries from node 1
        let request_message = node_2.receive_message().await;

        // handle append entries
        if let Ok(Message::AppendEntries { term, leader_id, new_entries }) = request_message {
            node_2.handle_append_entries(term, leader_id, &new_entries).await.unwrap();
        } else {
            panic!("Expected an AppendEntries message");
        }

        // node 1 receives append response from node 2
        let response_message = node_1.receive_message().await;

        if let Ok(Message::AppendResponse { term, success }) = response_message {
            assert_eq!(term, TERM);
            assert!(success);
        } else {
            panic!("Expected an AppendResponse message");
        }
    } else {
        panic!("Expected 2 nodes");
    }
}
