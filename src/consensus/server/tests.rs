use std::sync::Arc;

use tokio::sync::{Mutex, broadcast};

use crate::{
    config::Config,
    consensus::{ConsensusError, LogEntry, NodeServer, NodeState, NodeTimer},
    messaging::{Message, Network, NodeMessenger, NodeReceiver},
    state_machine::StateMachine,
};

// TODO: refactor tests to reduce boilerplate

/// Create a new node with a given id, messenger, and receiver.
fn create_node(id: u64, node_messenger: NodeMessenger) -> NodeServer {
    NodeServer::new(id, StateMachine::new(), node_messenger, broadcast::channel(16).0)
}

/// A struct to hold both a NodeServer and its NodeReceiver for testing
#[derive(Debug)]
pub struct TestNode {
    pub server: NodeServer,
    pub receiver: NodeReceiver,
}

/// Create a new network with a given number of nodes.
async fn create_network(number_of_nodes: usize) -> Vec<TestNode> {
    let network = Arc::new(Mutex::new(Network::new()));
    let mut nodes = Vec::new();
    for i in 0..number_of_nodes {
        let (node_messenger, node_receiver) = NodeMessenger::new(i as u64, network.clone());
        let server = create_node(i as u64, node_messenger.clone());
        network.lock().await.add_node(i as u64, node_messenger.sender.clone());
        nodes.push(TestNode { server, receiver: node_receiver });
    }

    nodes
}

/// Returns mutable references to the first two nodes in the slice.
/// Panics if there are fewer than two nodes.
fn get_two_nodes(
    nodes: &mut [TestNode],
) -> (&mut NodeServer, &mut NodeReceiver, &mut NodeServer, &mut NodeReceiver) {
    if let [node1, node2, ..] = nodes {
        (&mut node1.server, &mut node1.receiver, &mut node2.server, &mut node2.receiver)
    } else {
        panic!("Expected at least 2 nodes");
    }
}

/// Helper function to receive a message in tests
async fn receive_message(receiver: &mut NodeReceiver) -> Result<Arc<Message>, ConsensusError> {
    receiver.receive().await.map_err(ConsensusError::Transport)
}

/// Helper function to create a timer
fn create_timer() -> NodeTimer {
    let config = Config::default();
    NodeTimer::new(config)
}

#[tokio::test]
async fn test_node_broadcast_vote_request_fails_if_not_candidate() {
    const NODE_ID: u64 = 0;
    let mut nodes = create_network(1).await;
    let node = &mut nodes[NODE_ID as usize].server;

    assert_eq!(node.state(), NodeState::Follower);

    let result = node.broadcast_vote_request().await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), ConsensusError::NotCandidate(NODE_ID));
}

#[tokio::test]
async fn test_node_send_append_entries_to_all_followers_sends_message_to_all_nodes() {
    const NODE_ID: u64 = 0;
    const TERM: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_leader, _, node_follower, follower_receiver) = get_two_nodes(&mut nodes);

    // transition node 1 to leader
    node_leader.core.transition_to_candidate(); // sets term to 1
    node_leader.core.transition_to_leader(&[NODE_ID, node_follower.id()]); // does not change term, still 1

    // broadcast append entries
    let command = "test".to_string();
    let log_entry = LogEntry::new(TERM, command.clone());
    node_leader.core.leader_append_entry(command);
    node_leader.send_append_entries_to_all_followers().await.unwrap();

    // node 2 receives append entries from node 1
    let request_message = receive_message(follower_receiver).await;

    // check that the message is an append entries request
    if let Ok(msg_arc) = request_message {
        if let Message::AppendEntries {
            term,
            leader_id,
            ref entries,
            prev_log_index,
            prev_log_term,
            leader_commit,
        } = *msg_arc
        {
            assert_eq!(term, TERM);
            assert_eq!(leader_id, NODE_ID);
            assert_eq!(entries, &[log_entry]);
            assert_eq!(prev_log_index, 0);
            assert_eq!(prev_log_term, 0);
            assert_eq!(leader_commit, 0);
        } else {
            panic!("Expected an AppendEntries message");
        }
    }
}

#[tokio::test]
async fn test_node_broadcast_vote_request_sends_message_to_all_nodes() {
    const NODE_ID_1: u64 = 0;
    const TERM: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_leader, _, _, follower_receiver) = get_two_nodes(&mut nodes);

    // transition node 1 to candidate
    node_leader.core.transition_to_candidate();

    // broadcast vote request
    let result = node_leader.broadcast_vote_request().await;
    assert!(result.is_ok());

    // receive message from node 2
    let message = receive_message(follower_receiver).await;

    // check that the message is a vote request
    if let Ok(msg_arc) = message {
        if let Message::VoteRequest { term, candidate_id, last_log_index, last_log_term } = *msg_arc
        {
            assert_eq!(term, TERM);
            assert_eq!(candidate_id, NODE_ID_1);
            assert_eq!(last_log_index, 0);
            assert_eq!(last_log_term, 0);
        } else {
            panic!("Expected a VoteRequest message");
        }
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
    let (node_leader, leader_receiver, node_follower, follower_receiver) =
        get_two_nodes(&mut nodes);

    // transition node 1 to leader
    node_leader.core.transition_to_candidate(); // sets term to 1
    node_leader.core.transition_to_leader(&[node_leader.id(), node_follower.id()]); // does not change term, still 1

    // broadcast append entries
    let command = "test".to_string();
    node_leader.core.leader_append_entry(command.clone());
    node_leader.send_append_entries_to_all_followers().await.unwrap();

    // node 2 receives append entries from node 1
    let request_message = receive_message(follower_receiver).await;

    // handle append entries
    if let Ok(msg_arc) = request_message {
        if let Message::AppendEntries {
            term,
            leader_id,
            ref entries,
            prev_log_index,
            prev_log_term,
            leader_commit,
        } = *msg_arc
        {
            node_follower
                .handle_append_entries(
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                    &mut create_timer(),
                )
                .await
                .unwrap();
        } else {
            panic!("Expected an AppendEntries message");
        }
    } else {
        panic!("Expected an AppendEntries message");
    }

    // node 1 receives append response from node 2
    let response_message = receive_message(leader_receiver).await;

    if let Ok(msg_arc) = response_message {
        if let Message::AppendResponse { term, success, from_id } = *msg_arc {
            assert_eq!(term, TERM);
            assert!(success);
            assert_eq!(from_id, NODE_ID_2);
        } else {
            panic!("Expected an AppendResponse message");
        }
    } else {
        panic!("Expected an AppendResponse message");
    }
}

#[tokio::test]
async fn test_node_send_vote_response() {
    const NODE_ID_2: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_leader, leader_receiver, node_follower, follower_receiver) =
        get_two_nodes(&mut nodes);

    // transition node 1 to candidate
    node_leader.core.transition_to_candidate(); // sets term to 1

    // broadcast vote request
    let result = node_leader.broadcast_vote_request().await;
    assert!(result.is_ok());

    // receive message from node 2
    let message = receive_message(follower_receiver).await;

    // handle vote request
    if let Ok(msg_arc) = message {
        if let Message::VoteRequest { term, candidate_id, last_log_index, last_log_term } = *msg_arc
        {
            node_follower
                .handle_request_vote(
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                    &mut create_timer(),
                )
                .await
                .unwrap();
        } else {
            panic!("Expected a VoteRequest message");
        }
    } else {
        panic!("Expected a VoteRequest message");
    }

    // node 1 receives vote response from node 2
    let response_message = receive_message(leader_receiver).await;

    if let Ok(msg_arc) = response_message {
        if let Message::VoteResponse { term, vote_granted, from_id } = *msg_arc {
            assert_eq!(term, 1);
            assert!(vote_granted);
            assert_eq!(from_id, NODE_ID_2);
        } else {
            panic!("Expected a VoteResponse message");
        }
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
    let (node_leader, leader_receiver, node_follower, follower_receiver) =
        get_two_nodes(&mut nodes);

    // transition node 1 to candidate
    node_leader.core.transition_to_candidate();

    // set higher term on node 2
    node_follower.core.transition_to_follower(NODE_2_TERM);

    // node 1 broadcasts vote request
    let result = node_leader.broadcast_vote_request().await;
    assert!(result.is_ok());

    // node 2 receives vote request from node 1
    let message = receive_message(follower_receiver).await;

    // handle vote request from node 1
    if let Ok(msg_arc) = message {
        if let Message::VoteRequest { term, candidate_id, last_log_index, last_log_term } = *msg_arc
        {
            node_follower
                .handle_request_vote(
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                    &mut create_timer(),
                )
                .await
                .unwrap();

            // check that the vote response is a rejection
            let response_message = receive_message(leader_receiver).await;
            if let Ok(msg_arc) = response_message {
                if let Message::VoteResponse { term, vote_granted, from_id } = *msg_arc {
                    assert_eq!(term, NODE_2_TERM);
                    assert!(!vote_granted);
                    assert_eq!(from_id, NODE_ID_2);
                } else {
                    panic!("Expected a VoteResponse message");
                }
            } else {
                panic!("Expected a VoteResponse message");
            }
        } else {
            panic!("Expected a VoteRequest message");
        }
    }
}

#[tokio::test]
async fn test_node_handle_request_vote_accepts_newer_term() {
    const NODE_ID_2: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_leader, leader_receiver, node_follower, follower_receiver) =
        get_two_nodes(&mut nodes);

    // transition node 1 to candidate
    node_leader.core.transition_to_candidate(); // sets term to 1

    // set lower term on node 2
    node_follower.core.transition_to_follower(0);

    // node 1 broadcasts vote request
    let result = node_leader.broadcast_vote_request().await;
    assert!(result.is_ok());

    // node 2 receives vote request from node 1
    let message = receive_message(follower_receiver).await;

    // handle vote request from node 1
    if let Ok(msg_arc) = message {
        if let Message::VoteRequest { term, candidate_id, last_log_index, last_log_term } = *msg_arc
        {
            node_follower
                .handle_request_vote(
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                    &mut create_timer(),
                )
                .await
                .unwrap();

            // check that the vote response is a rejection
            let response_message = receive_message(leader_receiver).await;
            if let Ok(msg_arc) = response_message {
                if let Message::VoteResponse { term, vote_granted, from_id } = *msg_arc {
                    assert_eq!(term, 1);
                    assert!(vote_granted);
                    assert_eq!(from_id, NODE_ID_2);
                } else {
                    panic!("Expected a VoteResponse message");
                }
            } else {
                panic!("Expected a VoteResponse message");
            }
        } else {
            panic!("Expected a VoteRequest message");
        }
    }
}

#[tokio::test]
async fn test_node_handle_request_vote_accepts_equal_term() {
    const NODE_ID_2: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_leader, leader_receiver, node_follower, follower_receiver) =
        get_two_nodes(&mut nodes);

    // transition node 1 to candidate
    node_leader.core.transition_to_candidate(); // sets term to 1

    // set lower term on node 2
    node_follower.core.transition_to_follower(1);

    // node 1 broadcasts vote request
    let result = node_leader.broadcast_vote_request().await;
    assert!(result.is_ok());

    // node 2 receives vote request from node 1
    let message = receive_message(follower_receiver).await;

    // handle vote request from node 1
    if let Ok(msg_arc) = message {
        if let Message::VoteRequest { term, candidate_id, last_log_index, last_log_term } = *msg_arc
        {
            node_follower
                .handle_request_vote(
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                    &mut create_timer(),
                )
                .await
                .unwrap();

            // check that the vote response is a rejection
            let response_message = receive_message(leader_receiver).await;
            if let Ok(msg_arc) = response_message {
                if let Message::VoteResponse { term, vote_granted, from_id } = *msg_arc {
                    assert_eq!(term, 1);
                    assert!(vote_granted);
                    assert_eq!(from_id, NODE_ID_2);
                } else {
                    panic!("Expected a VoteResponse message");
                }
            } else {
                panic!("Expected a VoteResponse message");
            }
        } else {
            panic!("Expected a VoteRequest message");
        }
    }
}

#[tokio::test]
async fn test_node_handle_request_vote_rejects_if_already_voted() {
    const NODE_ID_2: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_leader, leader_receiver, node_follower, follower_receiver) =
        get_two_nodes(&mut nodes);

    // transition node 1 to candidate
    node_leader.core.transition_to_candidate(); // sets term to 1, votes for self

    // transition node 2 to candidate
    node_follower.core.transition_to_candidate(); // sets term to 1, votes for self

    // both should have same term and self as voted_for
    assert_eq!(node_leader.voted_for(), Some(node_leader.id()));
    assert_eq!(node_follower.voted_for(), Some(node_follower.id()));

    // node 1 broadcasts vote request
    let result = node_leader.broadcast_vote_request().await;
    assert!(result.is_ok());

    // node 2 receives vote request from node 1
    let message = receive_message(follower_receiver).await;

    // handle vote request from node 1
    if let Ok(msg_arc) = message {
        if let Message::VoteRequest { term, candidate_id, last_log_index, last_log_term } = *msg_arc
        {
            node_follower
                .handle_request_vote(
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                    &mut create_timer(),
                )
                .await
                .unwrap();

            // check that the vote response is a rejection
            let response_message = receive_message(leader_receiver).await;
            if let Ok(msg_arc) = response_message {
                if let Message::VoteResponse { term, vote_granted, from_id } = *msg_arc {
                    assert_eq!(term, 1);
                    assert!(!vote_granted);
                    assert_eq!(from_id, NODE_ID_2);
                } else {
                    panic!("Expected a VoteResponse message");
                }
            } else {
                panic!("Expected a VoteResponse message");
            }
        } else {
            panic!("Expected a VoteRequest message");
        }
    }
}

#[tokio::test]
async fn test_node_handle_request_vote_accepts_if_not_voted() {
    const NODE_ID_2: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_leader, leader_receiver, node_follower, follower_receiver) =
        get_two_nodes(&mut nodes);

    // transition node 1 to candidate
    node_leader.core.transition_to_candidate(); // sets term to 1

    // node 1 should have self as voted_for
    assert_eq!(node_leader.voted_for(), Some(node_leader.id()));

    // node 2 should not have voted yet
    assert_eq!(node_follower.voted_for(), None);

    // node 1 broadcasts vote request
    let result = node_leader.broadcast_vote_request().await;
    assert!(result.is_ok());

    // node 2 receives vote request from node 1
    let message = receive_message(follower_receiver).await;

    // handle vote request from node 1
    if let Ok(msg_arc) = message {
        if let Message::VoteRequest { term, candidate_id, last_log_index, last_log_term } = *msg_arc
        {
            node_follower
                .handle_request_vote(
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                    &mut create_timer(),
                )
                .await
                .unwrap();

            // check that the vote response is a rejection
            let response_message = receive_message(leader_receiver).await;
            if let Ok(msg_arc) = response_message {
                if let Message::VoteResponse { term, vote_granted, from_id } = *msg_arc {
                    assert_eq!(term, 1);
                    assert!(vote_granted);
                    assert_eq!(from_id, NODE_ID_2);
                } else {
                    panic!("Expected a VoteResponse message");
                }
            } else {
                panic!("Expected a VoteResponse message");
            }
        } else {
            panic!("Expected a VoteRequest message");
        }
    }
}

#[tokio::test]
async fn test_node_handle_append_entries_rejects_if_term_is_lower() {
    const NODE_1_TERM: u64 = 1;
    const NODE_2_TERM: u64 = NODE_1_TERM + 1; // higher term
    const NODE_ID_2: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_leader, leader_receiver, node_follower, follower_receiver) =
        get_two_nodes(&mut nodes);

    // transition node 1 to leader
    node_leader.core.transition_to_candidate(); // sets term to 1
    node_leader.core.transition_to_leader(&[node_leader.id(), node_follower.id()]); // does not change term, still 1

    // set lower term on node 2
    node_follower.core.transition_to_follower(NODE_2_TERM);

    // node 1 broadcasts append entries
    let command = "test".to_string();
    node_leader.core.leader_append_entry(command.clone());
    node_leader.send_append_entries_to_all_followers().await.unwrap();

    // node 2 receives append entries from node 1
    let request_message = receive_message(follower_receiver).await;

    // handle append entries
    if let Ok(msg_arc) = request_message {
        if let Message::AppendEntries {
            term,
            leader_id,
            ref entries,
            prev_log_index,
            prev_log_term,
            leader_commit,
        } = *msg_arc
        {
            node_follower
                .handle_append_entries(
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                    &mut create_timer(),
                )
                .await
                .unwrap();

            // check that the append response is a rejection
            let response_message = receive_message(leader_receiver).await;
            if let Ok(msg_arc) = response_message {
                if let Message::AppendResponse { term, success, from_id } = *msg_arc {
                    assert_eq!(term, NODE_2_TERM);
                    assert!(!success);
                    assert_eq!(from_id, NODE_ID_2);
                } else {
                    panic!("Expected an AppendResponse message");
                }
            } else {
                panic!("Expected an AppendResponse message");
            }
        } else {
            panic!("Expected an AppendEntries message");
        }
    }
}

#[tokio::test]
async fn test_node_handle_append_entries_accepts_if_term_is_higher() {
    const NODE_ID_2: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_leader, leader_receiver, node_follower, follower_receiver) =
        get_two_nodes(&mut nodes);

    // node 1 starts as follower, transition to candidate
    node_leader.core.transition_to_candidate(); // sets term to 1, votes for self

    // transition node 1 to leader
    node_leader.core.transition_to_leader(&[node_leader.id(), node_follower.id()]); // does not change term, still 1

    // set lower term on node 2
    node_follower.core.transition_to_follower(0); // sets node 2 term to lower than node 1

    // node 1 broadcasts append entries
    let command = "test".to_string();
    node_leader.core.leader_append_entry(command.clone());
    node_leader.send_append_entries_to_all_followers().await.unwrap();

    // node 2 receives append entries from node 1
    let request_message = receive_message(follower_receiver).await;

    // handle append entries
    if let Ok(msg_arc) = request_message {
        if let Message::AppendEntries {
            term,
            leader_id,
            ref entries,
            prev_log_index,
            prev_log_term,
            leader_commit,
        } = *msg_arc
        {
            node_follower
                .handle_append_entries(
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                    &mut create_timer(),
                )
                .await
                .unwrap();

            // check that the append response is a rejection
            let response_message = receive_message(leader_receiver).await;
            if let Ok(msg_arc) = response_message {
                if let Message::AppendResponse { term, success, from_id } = *msg_arc {
                    assert_eq!(term, 1);
                    assert!(success);
                    assert_eq!(from_id, NODE_ID_2);
                } else {
                    panic!("Expected an AppendResponse message");
                }
            } else {
                panic!("Expected an AppendResponse message");
            }
        } else {
            panic!("Expected an AppendEntries message");
        }
    }
}

#[tokio::test]
async fn test_node_handle_append_entries_accepts_if_term_is_equal() {
    const NODE_TERM: u64 = 1; // same term for both nodes
    const NODE_ID_2: u64 = 1;
    let mut nodes = create_network(2).await;

    // get the nodes
    let (node_leader, leader_receiver, node_follower, follower_receiver) =
        get_two_nodes(&mut nodes);

    // transition node 1 to leader
    node_leader.core.transition_to_candidate(); // sets term to 1
    node_leader.core.transition_to_leader(&[node_leader.id(), node_follower.id()]); // does not change term, still 1

    // set lower term on node 2
    node_follower.core.transition_to_follower(NODE_TERM);

    // node 1 broadcasts append entries
    let command = "test".to_string();
    node_leader.core.leader_append_entry(command.clone());
    node_leader.send_append_entries_to_all_followers().await.unwrap();

    // node 2 receives append entries from node 1
    let request_message = receive_message(follower_receiver).await;

    // handle append entries
    if let Ok(msg_arc) = request_message {
        if let Message::AppendEntries {
            term,
            leader_id,
            ref entries,
            prev_log_index,
            prev_log_term,
            leader_commit,
        } = *msg_arc
        {
            node_follower
                .handle_append_entries(
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                    &mut create_timer(),
                )
                .await
                .unwrap();

            // check that the append response is a rejection
            let response_message = receive_message(leader_receiver).await;
            if let Ok(msg_arc) = response_message {
                if let Message::AppendResponse { term, success, from_id } = *msg_arc {
                    assert_eq!(term, NODE_TERM);
                    assert!(success);
                    assert_eq!(from_id, NODE_ID_2);
                } else {
                    panic!("Expected an AppendResponse message");
                }
            } else {
                panic!("Expected an AppendResponse message");
            }
        } else {
            panic!("Expected an AppendEntries message");
        }
    }
}

#[tokio::test]
async fn test_node_start_append_entries_updates_log() {
    const NODE_ID: u64 = 0;
    const COMMAND: &str = "test command";
    let mut nodes = create_network(1).await;
    let node = &mut nodes[NODE_ID as usize].server;

    // start as follower, transition to candidate
    node.core.transition_to_candidate(); // sets term to 1, votes for self

    // transition to leader
    node.core.transition_to_leader(&[NODE_ID]); // does not change term, still 1

    // check default values
    assert_eq!(node.state(), NodeState::Leader);
    assert_eq!(node.log().len(), 0);
    assert_eq!(node.state_machine.get_state(), 0);

    // append to log
    node.start_append_entries(COMMAND.to_string()).await.unwrap();

    // check that the log has the new entry
    assert_eq!(node.log().len(), 1);
    assert_eq!(node.log()[0].term, 1);
    assert_eq!(node.log()[0].command, COMMAND);
}

#[tokio::test]
async fn test_node_start_append_entries_rejects_if_not_leader() {
    const NODE_ID: u64 = 0;
    const COMMAND: &str = "test command";
    let mut nodes = create_network(1).await;
    let node = &mut nodes[NODE_ID as usize].server;

    // check default values
    assert_eq!(node.state(), NodeState::Follower);
    assert_eq!(node.log().len(), 0);
    assert_eq!(node.state_machine.get_state(), 0);

    // append to log and broadcast
    let result = node.start_append_entries(COMMAND.to_string()).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), ConsensusError::NotLeader(NODE_ID));
}

#[tokio::test]
#[ignore = "Not implemented yet"]
async fn test_node_handle_append_entries_rejects_on_inconsistent_log() {
    // TODO: Implement this test once log consistency checks are implemented
    unimplemented!("Test not implemented yet");
}

#[tokio::test]
async fn test_node_process_message_handles_append_entries() {
    // Create a network with two nodes.
    let mut nodes = create_network(2).await;
    let (node_leader, leader_receiver, node_follower, _) = get_two_nodes(&mut nodes);

    // Set up the follower to have the same term.
    node_follower.core.transition_to_follower(1);

    // Create an AppendEntries message manually.
    let append_msg = Message::AppendEntries {
        term: 1,
        leader_id: node_leader.id(),
        entries: vec![LogEntry::new(1, "process".to_string())],
        prev_log_index: 0,
        prev_log_term: 1,
        leader_commit: 0,
    };

    // Process the message using the follower's process_message method.
    node_follower
        .process_message(std::sync::Arc::new(append_msg), &mut create_timer())
        .await
        .expect("process_message failed for AppendEntries");

    // Leader should receive the AppendResponse generated by processing.
    let response = receive_message(leader_receiver)
        .await
        .expect("Expected AppendResponse from process_message");
    if let Message::AppendResponse { term, success, from_id } = *response {
        assert_eq!(term, 1);
        assert!(success, "Expected AppendResponse to succeed");
        assert_eq!(from_id, node_follower.id());
    } else {
        panic!("Expected an AppendResponse message");
    }

    // Verify that the follower's log now contains the new entry.
    assert_eq!(node_follower.log().len(), 1);
    assert_eq!(node_follower.log()[0].command, "process".to_string());
}

#[tokio::test]
async fn test_node_process_message_vote_request() {
    let mut nodes = create_network(2).await;
    let (node_candidate, candidate_receiver, node_follower, _) = get_two_nodes(&mut nodes);

    // Prepare: ensure candidate is in Candidate state.
    node_candidate.core.transition_to_candidate();

    // Create a VoteRequest message (as if sent by the candidate).
    let vote_request = Message::VoteRequest {
        term: node_candidate.current_term(),
        candidate_id: node_candidate.id(),
        last_log_index: node_candidate.log_last_index(),
        last_log_term: node_candidate.log_last_term(),
    };

    // Process the VoteRequest on the follower.
    node_follower
        .process_message(std::sync::Arc::new(vote_request), &mut create_timer())
        .await
        .unwrap();

    // The candidate should receive a VoteResponse from the follower.
    let response =
        receive_message(candidate_receiver).await.expect("Expected VoteResponse message");
    if let Message::VoteResponse { term, vote_granted, from_id } = *response {
        assert_eq!(term, node_candidate.current_term());
        assert!(vote_granted, "Expected the vote to be granted");
        assert_eq!(from_id, node_follower.id());
    } else {
        panic!("Expected a VoteResponse message");
    }
}

#[tokio::test]
async fn test_node_process_message_vote_response() {
    let mut nodes = create_network(2).await;
    let (node_candidate, _, node_follower, follower_receiver) = get_two_nodes(&mut nodes);

    // Prepare: candidate starts an election.
    node_candidate.core.transition_to_candidate();

    // Create a VoteResponse message from the follower.
    let vote_response = Message::VoteResponse {
        term: node_candidate.current_term(),
        vote_granted: true,
        from_id: node_follower.id(),
    };

    // Process the VoteResponse on the candidate.
    node_candidate
        .process_message(std::sync::Arc::new(vote_response), &mut create_timer())
        .await
        .unwrap();

    // With 2 nodes, the candidate has already voted for itself.
    // So receiving one vote should form a majority.
    // As a result, the candidate should transition to Leader and broadcast an empty
    // AppendEntries message.
    let append_entries = receive_message(follower_receiver)
        .await
        .expect("Expected AppendEntries message after reaching majority");
    if let Message::AppendEntries {
        term,
        leader_id,
        ref entries,
        prev_log_index,
        prev_log_term,
        leader_commit,
    } = *append_entries
    {
        assert_eq!(term, node_candidate.current_term());
        assert_eq!(leader_id, node_candidate.id());
        assert!(entries.is_empty(), "Expected empty entries in the heartbeat AppendEntries");
        assert_eq!(leader_commit, 0);
        assert_eq!(prev_log_index, 0);
        assert_eq!(prev_log_term, 0);
    } else {
        panic!("Expected an AppendEntries message after majority vote");
    }
}

#[tokio::test]
async fn test_node_process_message_start_election_cmd_not_leader() {
    let mut nodes = create_network(2).await;
    let (node_candidate, _, _, follower_receiver) = get_two_nodes(&mut nodes);

    // Ensure the node is not a leader (i.e. still a Follower).
    assert_eq!(node_candidate.state(), NodeState::Follower);

    // Process a StartElectionCmd message.
    node_candidate
        .process_message(std::sync::Arc::new(Message::StartElectionCmd), &mut create_timer())
        .await
        .unwrap();

    // The node should have transitioned to Candidate.
    assert_eq!(node_candidate.state(), NodeState::Candidate);

    // The other node should receive a VoteRequest from the candidate.
    let vote_req = receive_message(follower_receiver).await.expect("Expected VoteRequest message");
    if let Message::VoteRequest { term, candidate_id, last_log_index, last_log_term } = *vote_req {
        assert_eq!(term, node_candidate.current_term());
        assert_eq!(candidate_id, node_candidate.id());
        assert_eq!(last_log_index, node_candidate.log_last_index());
        assert_eq!(last_log_term, node_candidate.log_last_term());
    } else {
        panic!("Expected a VoteRequest message");
    }
}

#[tokio::test]
async fn test_node_process_message_start_election_cmd_already_leader() {
    let mut nodes = create_network(1).await;
    let node = &mut nodes[0].server;

    // Transition the node to Leader.
    node.core.transition_to_candidate();
    node.core.transition_to_leader(&[node.id()]);

    // Process a StartElectionCmd message.
    node.process_message(std::sync::Arc::new(Message::StartElectionCmd), &mut create_timer())
        .await
        .unwrap();

    // Since the node is already a Leader, no new message should be broadcast.
    // Using a timeout to confirm that no message is received.
    use tokio::time::{Duration, timeout};
    let res = timeout(Duration::from_millis(100), receive_message(&mut nodes[0].receiver)).await;
    assert!(res.is_err(), "Expected no message to be sent when node is already leader");
}

#[tokio::test]
async fn test_node_process_message_start_append_entries_cmd_as_leader() {
    let mut nodes = create_network(2).await;
    let (node_leader, _, node_follower, follower_receiver) = get_two_nodes(&mut nodes);

    // Prepare: transition the node to Candidate then Leader.
    node_leader.core.transition_to_candidate();
    node_leader.core.transition_to_leader(&[node_leader.id(), node_follower.id()]);

    // Process a StartAppendEntriesCmd message with a command.
    let cmd = "test cmd".to_string();
    node_leader
        .process_message(
            std::sync::Arc::new(Message::StartAppendEntriesCmd { command: cmd.clone() }),
            &mut create_timer(),
        )
        .await
        .unwrap();

    // The leader's log should now contain the new entry.
    assert_eq!(node_leader.log().len(), 1);
    assert_eq!(node_leader.log()[0].command, cmd);
}

#[tokio::test]
async fn test_node_process_message_start_append_entries_cmd_not_leader() {
    let mut nodes = create_network(1).await;
    let node = &mut nodes[0].server;

    // Ensure the node is not a Leader.
    assert_eq!(node.state(), NodeState::Follower);

    // Process a StartAppendEntriesCmd message.
    let cmd = "test cmd".to_string();
    node.process_message(
        std::sync::Arc::new(Message::StartAppendEntriesCmd { command: cmd.clone() }),
        &mut create_timer(),
    )
    .await
    .unwrap();

    // Log should remain empty.
    assert_eq!(node.log().len(), 0);
}
