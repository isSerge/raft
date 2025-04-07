use std::sync::Arc;

use tokio::{
    sync::{Mutex, broadcast},
    time::{Duration, timeout},
};

use crate::{
    config::Config,
    consensus::{ConsensusError, LogEntry, NodeServer, NodeState, NodeTimer, TimerType},
    messaging::{Message, Network, NodeMessenger, NodeReceiver},
    state_machine::StateMachineDefault,
};

/// Create a new node with a given id, messenger, and receiver.
fn create_node(id: u64, node_messenger: NodeMessenger) -> NodeServer {
    NodeServer::new(
        id,
        Box::new(StateMachineDefault::new()),
        node_messenger,
        broadcast::channel(16).0,
    )
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

/// Returns mutable references to the first three nodes in the slice.
/// Panics if there are fewer than three nodes.
fn get_three_nodes(
    nodes: &mut [TestNode],
) -> (
    &mut NodeServer,
    &mut NodeReceiver,
    &mut NodeServer,
    &mut NodeReceiver,
    &mut NodeServer,
    &mut NodeReceiver,
) {
    if let [node1, node2, node3, ..] = nodes {
        (
            &mut node1.server,
            &mut node1.receiver,
            &mut node2.server,
            &mut node2.receiver,
            &mut node3.server,
            &mut node3.receiver,
        )
    } else {
        panic!("Expected at least 3 nodes");
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
async fn test_node_handle_append_entries_rejects_on_inconsistent_log() {
    // Scenario: Follower receives AppendEntries with inconsistent log (term
    // mismatch) and correctly rejects it.
    let total_nodes = 2;
    let mut nodes = create_network(total_nodes).await;
    let (follower, _, leader, _) = get_two_nodes(&mut nodes);

    // Setup: Make node 0 the leader, add a log entry
    leader.core.transition_to_candidate(); // term 1
    leader.core.leader_append_entry("cmd1".to_string()); // log index 1
    leader.core.transition_to_leader(&[leader.id(), follower.id()]); // Initializes next/match
    let term = leader.current_term(); // should be 1

    // Setup: Make follower have a different term at the same index
    follower.core.transition_to_candidate(); // term 1
    follower.core.transition_to_follower(term); // back to follower in term 1

    // Add a log entry to the follower's log
    let (consistent, modified) =
        follower.core.follower_append_entries(0, term, &[LogEntry::new(term, "cmd1".to_string())]);
    assert!(consistent);
    assert!(modified);

    // Verify initial state
    assert_eq!(follower.state(), NodeState::Follower);
    assert_eq!(follower.current_term(), term);
    assert_eq!(follower.log_last_index(), 1);
    assert_eq!(follower.log_last_term(), 1);

    // Action: Leader sends AppendEntries with a different term at index 1
    let leader_id = leader.id();
    let prev_log_index = 0;
    let prev_log_term = 0;
    let entries = vec![LogEntry::new(term, "cmd2".to_string())]; // term 1, different from follower's log
    let leader_commit = 0;

    // Process the message
    let msg = Message::AppendEntries {
        term,
        leader_id,
        entries: entries.clone(),
        prev_log_index,
        prev_log_term,
        leader_commit,
    };

    let result = follower.process_message(Arc::new(msg), &mut create_timer()).await;
    assert!(result.is_ok());

    // Assertions: Follower should reject the append due to log inconsistency
    // Follower's log should not have changed
    assert_eq!(follower.log_last_index(), 1);
    assert_eq!(follower.log_last_term(), 1);
    assert_eq!(follower.log()[0].command, "cmd1");
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
    let (node_leader, _, node_follower, _) = get_two_nodes(&mut nodes);

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

#[tokio::test]
async fn test_handle_timer_event_election_when_not_leader() {
    let mut nodes = create_network(2).await;
    let (node, _node_receiver, _, follower_receiver) = get_two_nodes(&mut nodes);
    let mut timer = create_timer();

    // Ensure node is a follower
    assert_eq!(node.state(), NodeState::Follower);

    // Handle election timer event
    node.handle_timer_event(TimerType::Election, &mut timer).await.unwrap();

    // Node should have transitioned to candidate
    assert_eq!(node.state(), NodeState::Candidate);

    // Verify that the timer was reset to election (candidates use election timer)
    assert_eq!(timer.get_active_timer(), TimerType::Election);

    // Node should have broadcast a vote request
    let vote_request = receive_message(follower_receiver).await.unwrap();
    if let Message::VoteRequest { term, candidate_id, last_log_index, last_log_term } =
        &*vote_request
    {
        assert_eq!(*term, 1);
        assert_eq!(*candidate_id, node.id());
        assert_eq!(*last_log_index, 0);
        assert_eq!(*last_log_term, 0);
    } else {
        panic!("Expected VoteRequest message");
    }
}

#[tokio::test]
async fn test_handle_timer_event_election_when_leader() {
    let mut nodes = create_network(2).await;
    let (node, _, _, _) = get_two_nodes(&mut nodes);
    let mut timer = create_timer();

    // Transition node to leader
    node.core.transition_to_candidate();
    node.core.transition_to_leader(&[node.id()]);

    // Ensure node is a leader
    assert_eq!(node.state(), NodeState::Leader);

    // Handle election timer event
    node.handle_timer_event(TimerType::Election, &mut timer).await.unwrap();

    // Node should still be a leader
    assert_eq!(node.state(), NodeState::Leader);

    // Verify that the timer was reset to heartbeat
    assert_eq!(timer.get_active_timer(), TimerType::Heartbeat);
}

#[tokio::test]
async fn test_handle_timer_event_heartbeat_when_not_leader() {
    let mut nodes = create_network(2).await;
    let (node, _, _, _) = get_two_nodes(&mut nodes);
    let mut timer = create_timer();

    // Ensure node is a follower
    assert_eq!(node.state(), NodeState::Follower);

    // Handle heartbeat timer event
    node.handle_timer_event(TimerType::Heartbeat, &mut timer).await.unwrap();

    // Node should still be a follower
    assert_eq!(node.state(), NodeState::Follower);

    // Verify that the timer was reset to election
    assert_eq!(timer.get_active_timer(), TimerType::Election);
}

#[tokio::test]
async fn test_handle_timer_event_heartbeat_after_election() {
    // Create a network with two nodes
    let mut nodes = create_network(2).await;
    let (node, node_receiver, follower, follower_receiver) = get_two_nodes(&mut nodes);
    let mut timer = create_timer();

    // Start election and wait for vote response
    node.process_message(Arc::new(Message::StartElectionCmd), &mut timer).await.unwrap();
    let vote_request = receive_message(follower_receiver).await.unwrap();
    follower.process_message(vote_request, &mut create_timer()).await.unwrap();
    let vote_response = receive_message(node_receiver).await.unwrap();
    node.process_message(vote_response, &mut timer).await.unwrap();

    // Ensure node is now a leader
    assert_eq!(node.state(), NodeState::Leader);
    assert_eq!(timer.get_active_timer(), TimerType::Heartbeat);

    // Handle heartbeat timer event
    node.handle_timer_event(TimerType::Heartbeat, &mut timer).await.unwrap();

    // Verify leader state and timer
    assert_eq!(node.state(), NodeState::Leader);
    assert_eq!(timer.get_active_timer(), TimerType::Heartbeat);

    // Verify heartbeat message
    let heartbeat = receive_message(follower_receiver).await.unwrap();
    if let Message::AppendEntries {
        term,
        leader_id,
        entries,
        prev_log_index,
        prev_log_term,
        leader_commit,
    } = &*heartbeat
    {
        assert_eq!(*term, 1);
        assert_eq!(*leader_id, node.id());
        assert!(entries.is_empty());
        assert_eq!(*prev_log_index, 0);
        assert_eq!(*prev_log_term, 0);
        assert_eq!(*leader_commit, 0);
    } else {
        panic!("Expected AppendEntries message");
    }
}

#[tokio::test]
async fn test_handle_vote_response_granted_no_majority() {
    let total_nodes = 2;
    let mut nodes = create_network(total_nodes).await;
    let candidate_id = 0;
    let candidate = &mut nodes[candidate_id as usize].server;

    // Start as candidate
    candidate.core.transition_to_candidate();
    assert_eq!(candidate.state(), NodeState::Candidate);
    assert_eq!(candidate.current_term(), 1);
    assert_eq!(candidate.core.votes_received(), 1); // Self-vote

    // Assertions: Still candidate, because only has one vote for itself - no
    // majority
    assert_eq!(candidate.state(), NodeState::Candidate);
    assert_eq!(candidate.current_term(), 1);
    assert_eq!(candidate.core.votes_received(), 1);
}
#[tokio::test]
async fn test_handle_vote_response_granted_reaches_majority() {
    // Scenario: 3 nodes, candidate needs 2 votes. Has self-vote, receives 2nd vote
    // -> becomes leader.
    let total_nodes = 3;
    let mut nodes = create_network(total_nodes).await;
    let candidate_id = 0;
    let voter_id = 1;
    let other_follower_id = 2;
    let (candidate, _, _, follower1_receiver, _, follower2_receiver) = get_three_nodes(&mut nodes);

    // Start as candidate
    candidate.core.transition_to_candidate();
    let term = candidate.current_term();
    assert_eq!(term, 1);
    assert_eq!(candidate.core.votes_received(), 1); // Self-vote

    // Action: Handle the vote that grants majority
    let result = candidate.handle_vote_response(term, voter_id, true, &mut create_timer()).await;
    assert!(result.is_ok());

    // Assertions: Transitioned to Leader
    assert_eq!(candidate.state(), NodeState::Leader);
    assert_eq!(candidate.current_term(), term);
    // votes_received is reset or irrelevant for leader
    assert!(candidate.match_index_for(voter_id).is_some()); // Leader state initialized
    assert!(candidate.match_index_for(other_follower_id).is_some());

    // Assertions: Initial heartbeat (empty AppendEntries) sent to followers
    let msg1 = timeout(Duration::from_millis(50), receive_message(follower1_receiver))
        .await
        .expect("Timeout waiting for heartbeat from new leader")
        .expect("Error receiving message");
    if let Message::AppendEntries { leader_id, entries, .. } = &*msg1 {
        assert_eq!(leader_id, &candidate_id);
        assert!(entries.is_empty());
    } else {
        panic!("Expected AppendEntries, got {:?}", msg1);
    }

    let msg2 = timeout(Duration::from_millis(50), receive_message(follower2_receiver))
        .await
        .expect("Timeout waiting for heartbeat from new leader")
        .expect("Error receiving message");
    if let Message::AppendEntries { leader_id, entries, .. } = &*msg2 {
        assert_eq!(*leader_id, candidate_id);
        assert!(entries.is_empty());
    } else {
        panic!("Expected AppendEntries, got {:?}", msg2);
    }
}

#[tokio::test]
async fn test_handle_vote_response_rejected() {
    // Scenario: 3 nodes, candidate receives a rejection.
    let total_nodes = 3;
    let mut nodes = create_network(total_nodes).await;
    let candidate_id = 0;
    let voter_id = 1;
    let candidate = &mut nodes[candidate_id as usize].server;

    // Start as candidate
    candidate.core.transition_to_candidate();
    let term = candidate.current_term();
    let initial_votes = candidate.core.votes_received();
    assert_eq!(candidate.state(), NodeState::Candidate);
    assert_eq!(term, 1);
    assert_eq!(initial_votes, 1); // Self-vote

    // Action: Handle one rejected vote
    let result = candidate.handle_vote_response(term, voter_id, false, &mut create_timer()).await;
    assert!(result.is_ok());

    // Assertions: Still candidate, vote count unchanged
    assert_eq!(candidate.state(), NodeState::Candidate);
    assert_eq!(candidate.current_term(), term);
    assert_eq!(candidate.core.votes_received(), initial_votes); // Vote count should not increase
}

#[tokio::test]
async fn test_handle_vote_response_stale_term() {
    // Scenario: Candidate is in term 2, receives a response for term 1.
    let mut nodes = create_network(1).await;
    let candidate_id = 0;
    let voter_id = 999;
    let candidate = &mut nodes[candidate_id as usize].server;

    // Start as candidate in term 2
    candidate.core.transition_to_candidate(); // Term 1
    candidate.core.transition_to_follower(2); // Force term update
    candidate.core.transition_to_candidate(); // Term 3, Vote 1
    let current_term = candidate.current_term();
    let initial_votes = candidate.core.votes_received();
    assert_eq!(candidate.state(), NodeState::Candidate);
    assert_eq!(current_term, 3);
    assert_eq!(initial_votes, 1);

    // Action: Handle response with older term (term 2)
    let result =
        candidate.handle_vote_response(current_term - 1, voter_id, true, &mut create_timer()).await;
    assert!(result.is_ok());

    // Assertions: State unchanged, response ignored
    assert_eq!(candidate.state(), NodeState::Candidate);
    assert_eq!(candidate.current_term(), current_term);
    assert_eq!(candidate.core.votes_received(), initial_votes);
}

#[tokio::test]
async fn test_handle_vote_response_future_term() {
    // Scenario: Candidate is in term 1, receives a response for term 2.
    let mut nodes = create_network(1).await;
    let voter_id = 999;
    let candidate_id = 0;
    let candidate = &mut nodes[candidate_id as usize].server;

    // Start as candidate in term 1
    candidate.core.transition_to_candidate();
    let initial_term = candidate.current_term();
    assert_eq!(candidate.state(), NodeState::Candidate);
    assert_eq!(initial_term, 1);

    let future_term = initial_term + 1;

    // Action: Handle response with future term (vote granted status doesn't matter)
    let result =
        candidate.handle_vote_response(future_term, voter_id, false, &mut create_timer()).await;
    assert!(result.is_ok());

    // Assertions: State becomes Follower, term updated
    assert_eq!(candidate.state(), NodeState::Follower);
    assert_eq!(candidate.current_term(), future_term);
    // votes_received should be reset upon transitioning
    assert_eq!(candidate.core.votes_received(), 0);
}

#[tokio::test]
async fn test_handle_vote_response_when_not_candidate() {
    // Scenario: Node is Follower, receives a VoteResponse.
    let mut nodes = create_network(1).await;
    let node_id = 0;
    let node = &mut nodes[node_id as usize].server;

    // Ensure state is Follower
    assert_eq!(node.state(), NodeState::Follower);
    let initial_term = node.current_term();

    // Action: Handle VoteResponse
    let result = node.handle_vote_response(initial_term, 99, true, &mut create_timer()).await;
    assert!(result.is_ok());

    // Assertions: State remains Follower, term unchanged
    assert_eq!(node.state(), NodeState::Follower);
    assert_eq!(node.current_term(), initial_term);
    assert_eq!(node.core.votes_received(), 0);
}

#[tokio::test]
async fn test_handle_append_response_success_updates_match_next() {
    // Scenario: Leader receives successful response, updates match/next for the
    // follower.
    let total_nodes = 2; // Leader + 1 Follower
    let mut nodes = create_network(total_nodes).await;
    let leader_id = 0;
    let follower_id = 1;
    let leader = &mut nodes[leader_id as usize].server;

    // Setup: Make node 0 the leader, add a log entry
    leader.core.transition_to_candidate(); // term 1
    leader.core.leader_append_entry("cmd1".to_string()); // log index 1
    leader.core.transition_to_leader(&[leader_id, follower_id]); // Initializes next/match
    let term = leader.current_term(); // should be 1

    // Assume leader sent entry 1 (prevLogIndex=0, entries.len=1)
    let sent_prev_log_index = 0;
    let sent_entries_len = 1;
    leader.pending_append_entries.insert(follower_id, (sent_prev_log_index, sent_entries_len));

    let initial_match = leader.match_index_for(follower_id).unwrap();
    let initial_next = leader.next_index_for(follower_id).unwrap();

    // Action: Process successful response from follower
    let result = leader.handle_append_response(term, true, follower_id, &mut create_timer()).await;
    assert!(result.is_ok());

    // Assertions: Leader state, match/next updated, commit unchanged
    assert_eq!(leader.state(), NodeState::Leader);
    assert_eq!(
        leader.match_index_for(follower_id).unwrap(),
        sent_prev_log_index + sent_entries_len as u64
    );
    assert_eq!(
        leader.next_index_for(follower_id).unwrap(),
        sent_prev_log_index + sent_entries_len as u64 + 1
    );
    assert_ne!(leader.match_index_for(follower_id).unwrap(), initial_match); // Ensure it changed
    assert_ne!(leader.next_index_for(follower_id).unwrap(), initial_next); // Ensure it changed
    assert_eq!(leader.commit_index(), 0); // Need 2/2 matches for commit index 1
    assert!(!leader.pending_append_entries.contains_key(&follower_id)); // Pending info cleared
}

#[tokio::test]
async fn test_handle_append_response_stale_term() {
    // Scenario: Leader (term 2) receives response for older term (term 1). Should
    // ignore.
    let total_nodes = 2;
    let mut nodes = create_network(total_nodes).await;

    let (leader, _, follower, _) = get_two_nodes(&mut nodes);

    // Setup: Leader term 2
    leader.core.transition_to_candidate(); // term 1
    leader.core.transition_to_follower(2); // force term 2
    leader.core.transition_to_candidate(); // term 3
    leader.core.transition_to_leader(&[leader.id(), follower.id()]); // leader in term 3
    let current_term = leader.current_term(); // 3
    let stale_term = current_term - 1; // 2

    let initial_match = leader.match_index_for(follower.id()).unwrap();
    let initial_next = leader.next_index_for(follower.id()).unwrap();

    // Add pending info to see if it gets cleared (it shouldn't)
    leader.pending_append_entries.insert(follower.id(), (0, 1));

    // Action: Process response with stale term
    let result =
        leader.handle_append_response(stale_term, true, follower.id(), &mut create_timer()).await;
    assert!(result.is_ok());

    // Assertions: State unchanged, response ignored, pending info NOT cleared
    assert_eq!(leader.state(), NodeState::Leader);
    assert_eq!(leader.current_term(), current_term);
    assert_eq!(leader.match_index_for(follower.id()).unwrap(), initial_match);
    assert_eq!(leader.next_index_for(follower.id()).unwrap(), initial_next);
    assert_eq!(leader.commit_index(), 0);
    assert!(leader.pending_append_entries.contains_key(&follower.id())); // Pending info remains
}

#[tokio::test]
async fn test_handle_append_response_future_term() {
    // Scenario: Leader (term 1) receives response for future term (term 2). Should
    // step down.
    let total_nodes = 2;
    let mut nodes = create_network(total_nodes).await;
    let (leader, _, follower, _) = get_two_nodes(&mut nodes);

    // Setup: Leader term 1
    leader.core.transition_to_candidate(); // term 1
    leader.core.transition_to_leader(&[leader.id(), follower.id()]);
    let initial_term = leader.current_term(); // 1
    let future_term = initial_term + 1; // 2

    // Add pending info to see if it gets cleared (it should)
    leader.pending_append_entries.insert(follower.id(), (0, 1));

    // Action: Process response with future term (success flag doesn't matter)
    let result =
        leader.handle_append_response(future_term, false, follower.id(), &mut create_timer()).await;
    assert!(result.is_ok());

    // Assertions: State becomes Follower, term updated, pending info cleared
    assert_eq!(leader.state(), NodeState::Follower);
    assert_eq!(leader.current_term(), future_term);
    assert!(leader.pending_append_entries.is_empty()); // All pending info cleared on step down
}

#[tokio::test]
async fn test_handle_append_response_when_not_leader() {
    // Scenario: Node is Follower, receives an AppendResponse. Should ignore.
    let total_nodes = 2;
    let mut nodes = create_network(total_nodes).await;
    let (node, _, follower, _) = get_two_nodes(&mut nodes);

    // Setup: Ensure node is Follower, add some dummy pending info
    assert_eq!(node.state(), NodeState::Follower);
    let initial_term = node.current_term(); // 0
    node.pending_append_entries.insert(follower.id(), (0, 1));

    // Action: Process AppendResponse while Follower
    let result =
        node.handle_append_response(initial_term, true, follower.id(), &mut create_timer()).await;
    assert!(result.is_ok());

    // Assertions: State remains Follower, term unchanged, pending info for that
    // follower cleared
    assert_eq!(node.state(), NodeState::Follower);
    assert_eq!(node.current_term(), initial_term);
    assert!(!node.pending_append_entries.contains_key(&follower.id())); // Clears for the specific follower
}

#[tokio::test]
async fn test_handle_append_response_no_pending_info() {
    // Scenario: Leader receives a response but has no pending info for it (e.g.,
    // duplicate). Should ignore update.
    let total_nodes = 2;
    let mut nodes = create_network(total_nodes).await;
    let (leader, _, follower, _) = get_two_nodes(&mut nodes);

    // Setup: Leader term 1, NO pending info
    leader.core.transition_to_candidate(); // term 1
    leader.core.transition_to_leader(&[leader.id(), follower.id()]);
    let term = leader.current_term();

    let initial_match = leader.match_index_for(follower.id()).unwrap();
    let initial_next = leader.next_index_for(follower.id()).unwrap();
    assert!(!leader.pending_append_entries.contains_key(&follower.id())); // Verify no pending info

    // Action: Process successful response from follower
    let result =
        leader.handle_append_response(term, true, follower.id(), &mut create_timer()).await;
    assert!(result.is_ok());

    // Assertions: Leader state, match/next/commit unchanged because no pending info
    // was found
    assert_eq!(leader.state(), NodeState::Leader);
    assert_eq!(leader.match_index_for(follower.id()).unwrap(), initial_match); // Unchanged
    assert_eq!(leader.next_index_for(follower.id()).unwrap(), initial_next); // Unchanged
    assert_eq!(leader.commit_index(), 0);
    assert!(!leader.pending_append_entries.contains_key(&follower.id())); // Still none
}

#[tokio::test]
#[ignore]
async fn test_handle_append_response_success_advances_commit_n3() {
    unimplemented!()
}

#[tokio::test]
#[ignore]
async fn test_handle_append_response_failure_decrements_next() {
    unimplemented!()
}
