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
        let (node_messenger, node_receiver) = NodeMessenger::new(i as u64, network.clone());
        let node = create_node(i as u64, node_messenger.clone(), node_receiver);
        network.lock().await.add_node(i as u64, node_messenger.sender.clone());
        nodes.push(node);
    }

    nodes
}

/// Returns mutable references to the first two nodes in the slice.
/// Panics if there are fewer than two nodes.
fn get_two_nodes(nodes: &mut [Node]) -> (&mut Node, &mut Node) {
    if let [node1, node2, ..] = nodes {
        (node1, node2)
    } else {
        panic!("Expected at least 2 nodes");
    }
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
    assert_eq!(node.votes_received(), 0);

    node.transition_to(NodeState::Candidate, TERM);

    assert_eq!(node.state(), NodeState::Candidate);
    assert_eq!(node.current_term(), TERM);
    assert_eq!(node.voted_for(), Some(NODE_ID));
    assert_eq!(node.votes_received(), 1);
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
    assert_eq!(node.votes_received(), 0);

    node.transition_to(NodeState::Candidate, TERM);

    // check values after transition
    assert_eq!(node.state(), NodeState::Candidate);
    assert_eq!(node.current_term(), TERM);
    assert_eq!(node.voted_for(), Some(NODE_ID));
    assert_eq!(node.votes_received(), 1);

    const NEW_TERM: u64 = TERM + 1; // Increment the term to trigger a transition to follower

    node.transition_to(NodeState::Follower, NEW_TERM);

    assert_eq!(node.state(), NodeState::Follower);
    assert_eq!(node.current_term(), NEW_TERM);
    assert_eq!(node.voted_for(), None);
    assert_eq!(node.votes_received(), 0);
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
async fn test_node_broadcast_append_entries_sends_message_to_all_nodes() {
    const NODE_ID: u64 = 0;
    const TERM: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_1, node_2) = get_two_nodes(&mut nodes);

    // transition node 1 to leader
    node_1.transition_to(NodeState::Leader, TERM);

    // broadcast append entries
    let log_entry = LogEntry::new(TERM, "test".to_string());
    node_1.broadcast_append_entries(vec![log_entry.clone()]).await.unwrap();

    // node 2 receives append entries from node 1
    let request_message = node_2.receive_message().await;

    // check that the message is an append entries request
    if let Ok(Message::AppendEntries { term, leader_id, new_entries, commit_index }) =
        request_message
    {
        assert_eq!(term, TERM);
        assert_eq!(leader_id, NODE_ID);
        assert_eq!(new_entries, vec![log_entry]);
        assert_eq!(commit_index, 0);
    } else {
        panic!("Expected an AppendEntries message");
    }
}

#[tokio::test]
async fn test_node_broadcast_vote_request_sends_message_to_all_nodes() {
    const NODE_ID_1: u64 = 0;
    const TERM: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_1, node_2) = get_two_nodes(&mut nodes);

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
}

#[tokio::test]
async fn test_node_send_append_response() {
    const TERM: u64 = 1;
    const NODE_ID_2: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_1, node_2) = get_two_nodes(&mut nodes);

    // transition node 1 to leader
    node_1.transition_to(NodeState::Leader, TERM);

    // broadcast append entries
    let log_entry = LogEntry::new(TERM, "test".to_string());
    node_1.broadcast_append_entries(vec![log_entry]).await.unwrap();

    // node 2 receives append entries from node 1
    let request_message = node_2.receive_message().await;

    // handle append entries
    if let Ok(Message::AppendEntries { term, leader_id, new_entries, commit_index }) =
        request_message
    {
        node_2.handle_append_entries(term, leader_id, &new_entries, commit_index).await.unwrap();
    } else {
        panic!("Expected an AppendEntries message");
    }

    // node 1 receives append response from node 2
    let response_message = node_1.receive_message().await;

    if let Ok(Message::AppendResponse { term, success, from_id }) = response_message {
        assert_eq!(term, TERM);
        assert!(success);
        assert_eq!(from_id, NODE_ID_2);
    } else {
        panic!("Expected an AppendResponse message");
    }
}

#[tokio::test]
async fn test_node_send_vote_response() {
    const TERM: u64 = 21;
    const NODE_ID_2: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_1, node_2) = get_two_nodes(&mut nodes);

    // transition node 1 to candidate
    node_1.transition_to(NodeState::Candidate, TERM);

    // broadcast vote request
    let result = node_1.broadcast_vote_request().await;
    assert!(result.is_ok());

    // receive message from node 2
    let message = node_2.receive_message().await;

    // handle vote request
    if let Ok(Message::VoteRequest { term, candidate_id }) = message {
        node_2.handle_request_vote(term, candidate_id).await.unwrap();
    } else {
        panic!("Expected a VoteRequest message");
    }

    // node 1 receives vote response from node 2
    let response_message = node_1.receive_message().await;

    if let Ok(Message::VoteResponse { term, vote_granted, from_id }) = response_message {
        assert_eq!(term, TERM);
        assert!(vote_granted);
        assert_eq!(from_id, NODE_ID_2);
    } else {
        panic!("Expected a VoteResponse message");
    }
}

#[tokio::test]
async fn test_node_handle_request_vote_rejects_older_term() {
    const NODE_1_TERM: u64 = 1;
    const NODE_2_TERM: u64 = NODE_1_TERM + 1;
    const NODE_ID_2: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_1, node_2) = get_two_nodes(&mut nodes);

    // transition node 1 to candidate
    node_1.transition_to(NodeState::Candidate, NODE_1_TERM);

    // set higher term on node 2
    node_2.transition_to(NodeState::Follower, NODE_2_TERM);

    // node 1 broadcasts vote request
    let result = node_1.broadcast_vote_request().await;
    assert!(result.is_ok());

    // node 2 receives vote request from node 1
    let message = node_2.receive_message().await;

    // handle vote request from node 1
    if let Ok(Message::VoteRequest { term, candidate_id }) = message {
        node_2.handle_request_vote(term, candidate_id).await.unwrap();

        // check that the vote response is a rejection
        let response_message = node_1.receive_message().await;
        if let Ok(Message::VoteResponse { term, vote_granted, from_id }) = response_message {
            assert_eq!(term, NODE_2_TERM);
            assert!(!vote_granted);
            assert_eq!(from_id, NODE_ID_2);
        } else {
            panic!("Expected a VoteResponse message");
        }
    } else {
        panic!("Expected a VoteRequest message");
    }
}

#[tokio::test]
async fn test_node_handle_request_vote_accepts_newer_term() {
    const NODE_1_TERM: u64 = 10;
    const NODE_2_TERM: u64 = NODE_1_TERM - 1;
    const NODE_ID_2: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_1, node_2) = get_two_nodes(&mut nodes);

    // transition node 1 to candidate
    node_1.transition_to(NodeState::Candidate, NODE_1_TERM);

    // set lower term on node 2
    node_2.transition_to(NodeState::Follower, NODE_2_TERM);

    // node 1 broadcasts vote request
    let result = node_1.broadcast_vote_request().await;
    assert!(result.is_ok());

    // node 2 receives vote request from node 1
    let message = node_2.receive_message().await;

    // handle vote request from node 1
    if let Ok(Message::VoteRequest { term, candidate_id }) = message {
        node_2.handle_request_vote(term, candidate_id).await.unwrap();

        // check that the vote response is a rejection
        let response_message = node_1.receive_message().await;
        if let Ok(Message::VoteResponse { term, vote_granted, from_id }) = response_message {
            assert_eq!(term, NODE_1_TERM);
            assert!(vote_granted);
            assert_eq!(from_id, NODE_ID_2);
        } else {
            panic!("Expected a VoteResponse message");
        }
    } else {
        panic!("Expected a VoteRequest message");
    }
}

#[tokio::test]
async fn test_node_handle_request_vote_accepts_equal_term() {
    const NODE_1_TERM: u64 = 10;
    const NODE_2_TERM: u64 = NODE_1_TERM;
    const NODE_ID_2: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_1, node_2) = get_two_nodes(&mut nodes);

    // transition node 1 to candidate
    node_1.transition_to(NodeState::Candidate, NODE_1_TERM);

    // set lower term on node 2
    node_2.transition_to(NodeState::Follower, NODE_2_TERM);

    // node 1 broadcasts vote request
    let result = node_1.broadcast_vote_request().await;
    assert!(result.is_ok());

    // node 2 receives vote request from node 1
    let message = node_2.receive_message().await;

    // handle vote request from node 1
    if let Ok(Message::VoteRequest { term, candidate_id }) = message {
        node_2.handle_request_vote(term, candidate_id).await.unwrap();

        // check that the vote response is a rejection
        let response_message = node_1.receive_message().await;
        if let Ok(Message::VoteResponse { term, vote_granted, from_id }) = response_message {
            assert_eq!(term, NODE_1_TERM);
            assert!(vote_granted);
            assert_eq!(from_id, NODE_ID_2);
        } else {
            panic!("Expected a VoteResponse message");
        }
    } else {
        panic!("Expected a VoteRequest message");
    }
}

#[tokio::test]
async fn test_node_handle_request_vote_rejects_if_already_voted() {
    const NODE_1_TERM: u64 = 10;
    const NODE_2_TERM: u64 = NODE_1_TERM;
    const NODE_ID_2: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_1, node_2) = get_two_nodes(&mut nodes);

    // transition node 1 to candidate
    node_1.transition_to(NodeState::Candidate, NODE_1_TERM);

    // transition node 2 to follower
    node_2.transition_to(NodeState::Candidate, NODE_2_TERM);

    // both should have same term and self as voted_for
    assert_eq!(node_1.voted_for(), Some(node_1.id()));
    assert_eq!(node_2.voted_for(), Some(node_2.id()));

    // node 1 broadcasts vote request
    let result = node_1.broadcast_vote_request().await;
    assert!(result.is_ok());

    // node 2 receives vote request from node 1
    let message = node_2.receive_message().await;

    // handle vote request from node 1
    if let Ok(Message::VoteRequest { term, candidate_id }) = message {
        node_2.handle_request_vote(term, candidate_id).await.unwrap();

        // check that the vote response is a rejection
        let response_message = node_1.receive_message().await;
        if let Ok(Message::VoteResponse { term, vote_granted, from_id }) = response_message {
            assert_eq!(term, NODE_1_TERM);
            assert!(!vote_granted);
            assert_eq!(from_id, NODE_ID_2);
        } else {
            panic!("Expected a VoteResponse message");
        }
    } else {
        panic!("Expected a VoteRequest message");
    }
}

#[tokio::test]
async fn test_node_handle_request_vote_accepts_if_not_voted() {
    const NODE_1_TERM: u64 = 10;
    const NODE_ID_2: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_1, node_2) = get_two_nodes(&mut nodes);

    // transition node 1 to candidate
    node_1.transition_to(NodeState::Candidate, NODE_1_TERM);

    // node 1 should have self as voted_for
    assert_eq!(node_1.voted_for(), Some(node_1.id()));

    // node 2 should not have voted yet
    assert_eq!(node_2.voted_for(), None);

    // node 1 broadcasts vote request
    let result = node_1.broadcast_vote_request().await;
    assert!(result.is_ok());

    // node 2 receives vote request from node 1
    let message = node_2.receive_message().await;

    // handle vote request from node 1
    if let Ok(Message::VoteRequest { term, candidate_id }) = message {
        node_2.handle_request_vote(term, candidate_id).await.unwrap();

        // check that the vote response is a rejection
        let response_message = node_1.receive_message().await;
        if let Ok(Message::VoteResponse { term, vote_granted, from_id }) = response_message {
            assert_eq!(term, NODE_1_TERM);
            assert!(vote_granted);
            assert_eq!(from_id, NODE_ID_2);
        } else {
            panic!("Expected a VoteResponse message");
        }
    } else {
        panic!("Expected a VoteRequest message");
    }
}

#[tokio::test]
async fn test_node_handle_append_entries_rejects_if_term_is_lower() {
    const NODE_1_TERM: u64 = 10;
    const NODE_2_TERM: u64 = NODE_1_TERM + 1; // higher term
    const NODE_ID_2: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_1, node_2) = get_two_nodes(&mut nodes);

    // transition node 1 to leader
    node_1.transition_to(NodeState::Leader, NODE_1_TERM);

    // set lower term on node 2
    node_2.transition_to(NodeState::Follower, NODE_2_TERM);

    // node 1 broadcasts append entries
    let log_entry = LogEntry::new(NODE_1_TERM, "test".to_string());
    node_1.broadcast_append_entries(vec![log_entry.clone()]).await.unwrap();

    // node 2 receives append entries from node 1
    let request_message = node_2.receive_message().await;

    // handle append entries
    if let Ok(Message::AppendEntries { term, leader_id, new_entries, commit_index }) =
        request_message
    {
        node_2.handle_append_entries(term, leader_id, &new_entries, commit_index).await.unwrap();

        // check that the append response is a rejection
        let response_message = node_1.receive_message().await;
        if let Ok(Message::AppendResponse { term, success, from_id }) = response_message {
            assert_eq!(term, NODE_2_TERM);
            assert!(!success);
            assert_eq!(from_id, NODE_ID_2);
        } else {
            panic!("Expected an AppendResponse message");
        }
    } else {
        panic!("Expected an AppendEntries message");
    }
}

#[tokio::test]
async fn test_node_handle_append_entries_accepts_if_term_is_higher() {
    const NODE_1_TERM: u64 = 10; // higher term
    const NODE_2_TERM: u64 = NODE_1_TERM - 1;
    const NODE_ID_2: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_1, node_2) = get_two_nodes(&mut nodes);

    // transition node 1 to leader
    node_1.transition_to(NodeState::Leader, NODE_1_TERM);

    // set lower term on node 2
    node_2.transition_to(NodeState::Follower, NODE_2_TERM);

    // node 1 broadcasts append entries
    let log_entry = LogEntry::new(NODE_1_TERM, "test".to_string());
    node_1.broadcast_append_entries(vec![log_entry.clone()]).await.unwrap();

    // node 2 receives append entries from node 1
    let request_message = node_2.receive_message().await;

    // handle append entries
    if let Ok(Message::AppendEntries { term, leader_id, new_entries, commit_index }) =
        request_message
    {
        node_2.handle_append_entries(term, leader_id, &new_entries, commit_index).await.unwrap();

        // check that the append response is a rejection
        let response_message = node_1.receive_message().await;
        if let Ok(Message::AppendResponse { term, success, from_id }) = response_message {
            assert_eq!(term, NODE_1_TERM);
            assert!(success);
            assert_eq!(from_id, NODE_ID_2);
        } else {
            panic!("Expected an AppendResponse message");
        }
    } else {
        panic!("Expected an AppendEntries message");
    }
}

#[tokio::test]
async fn test_node_handle_append_entries_accepts_if_term_is_equal() {
    const NODE_TERM: u64 = 10; // same term for both nodes
    const NODE_ID_2: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_1, node_2) = get_two_nodes(&mut nodes);

    // transition node 1 to leader
    node_1.transition_to(NodeState::Leader, NODE_TERM);

    // set lower term on node 2
    node_2.transition_to(NodeState::Follower, NODE_TERM);

    // node 1 broadcasts append entries
    let log_entry = LogEntry::new(NODE_TERM, "test".to_string());
    node_1.broadcast_append_entries(vec![log_entry.clone()]).await.unwrap();

    // node 2 receives append entries from node 1
    let request_message = node_2.receive_message().await;

    // handle append entries
    if let Ok(Message::AppendEntries { term, leader_id, new_entries, commit_index }) =
        request_message
    {
        node_2.handle_append_entries(term, leader_id, &new_entries, commit_index).await.unwrap();

        // check that the append response is a rejection
        let response_message = node_1.receive_message().await;
        if let Ok(Message::AppendResponse { term, success, from_id }) = response_message {
            assert_eq!(term, NODE_TERM);
            assert!(success);
            assert_eq!(from_id, NODE_ID_2);
        } else {
            panic!("Expected an AppendResponse message");
        }
    } else {
        panic!("Expected an AppendEntries message");
    }
}

#[tokio::test]
async fn test_node_append_to_log_and_broadcast_updates_log_and_state_machine() {
    const NODE_ID: u64 = 0;
    const TERM: u64 = 1;
    const COMMAND: &str = "test command";
    let mut nodes = create_network(1).await;
    let node = &mut nodes[NODE_ID as usize];

    // transition to leader
    node.transition_to(NodeState::Leader, TERM);

    // check default values
    assert_eq!(node.log().len(), 0);
    assert_eq!(node.state_machine.get_state(), 0);

    // append to log and broadcast
    node.append_to_log_and_broadcast(COMMAND.to_string()).await.unwrap();

    // check that the log has the new entry
    assert_eq!(node.log().len(), 1);
    assert_eq!(node.log()[0].term, TERM);
    assert_eq!(node.log()[0].command, COMMAND);
    // check that the state machine value was incremented
    assert_eq!(node.state_machine.get_state(), 1);
}

#[tokio::test]
async fn test_node_append_to_log_and_broadcast_rejects_if_not_leader() {
    const NODE_ID: u64 = 0;
    const COMMAND: &str = "test command";
    let mut nodes = create_network(1).await;
    let node = &mut nodes[NODE_ID as usize];

    // check default values
    assert_eq!(node.state(), NodeState::Follower);
    assert_eq!(node.log().len(), 0);
    assert_eq!(node.state_machine.get_state(), 0);

    // append to log and broadcast
    let result = node.append_to_log_and_broadcast(COMMAND.to_string()).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), ConsensusError::NotLeader(NODE_ID));
}

#[tokio::test]
async fn test_node_append_to_log_and_broadcast_sends_append_entries_to_all_nodes() {
    const NODE_ID: u64 = 0;
    const TERM: u64 = 1;
    const COMMAND: &str = "test command";
    let mut nodes = create_network(2).await;
    let (node_1, node_2) = get_two_nodes(&mut nodes);

    // transition node 1 to leader
    node_1.transition_to(NodeState::Leader, TERM);

    // append to log and broadcast
    node_1.append_to_log_and_broadcast(COMMAND.to_string()).await.unwrap();

    // check that the log has the new entry
    assert_eq!(node_1.log().len(), 1);
    assert_eq!(node_1.log()[0].term, TERM);
    assert_eq!(node_1.log()[0].command, COMMAND);

    // check that node 2 received the append entries
    let message = node_2.receive_message().await;

    if let Ok(Message::AppendEntries { term, leader_id, new_entries, commit_index }) = message {
        assert_eq!(term, TERM);
        assert_eq!(leader_id, NODE_ID);
        assert_eq!(new_entries.len(), node_1.log().len());
        assert_eq!(new_entries[0].term, TERM);
        assert_eq!(new_entries[0].command, COMMAND);
        assert_eq!(commit_index, 1);
    } else {
        panic!("Expected an AppendEntries message");
    }
}
