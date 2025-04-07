use crate::consensus::{
    LogEntry,
    core::{NodeCore, NodeState},
};

const NODE_ID: u64 = 0;
const PEER_ID: u64 = 1;
const PEER_ID_2: u64 = 2;
const PEER_ID_3: u64 = 3;
const PEER_ID_4: u64 = 4;

// --- Helper function to create a leader core for tests ---
fn setup_leader_core(id: u64, term: u64, log: Vec<LogEntry>, peers: &[u64]) -> NodeCore {
    let mut core = NodeCore::new(id);
    core.current_term = term;
    core.log = log;
    // Mimic transition without needing Candidate state votes
    core.state = NodeState::Leader;
    core.initialize_leader_state(peers);
    core
}

#[test]
fn test_core_transition_to_candidate_and_vote_for_self() {
    const TERM: u64 = 1;
    let mut core = NodeCore::new(NODE_ID);

    // check default values
    assert_eq!(core.state(), NodeState::Follower);
    assert_eq!(core.current_term(), 0);
    assert_eq!(core.voted_for(), None);

    core.transition_to_candidate();

    assert_eq!(core.state(), NodeState::Candidate);
    assert_eq!(core.current_term(), TERM);
    assert_eq!(core.voted_for(), Some(NODE_ID));
    assert_eq!(core.votes_received(), 1);
}

#[test]
fn test_core_transition_to_candidate_does_not_transition_if_leader() {
    const TERM: u64 = 1;

    let mut core = NodeCore::new(NODE_ID);

    core.transition_to_candidate();
    core.transition_to_leader(&[NODE_ID]);

    assert_eq!(core.state(), NodeState::Leader);
    assert_eq!(core.current_term(), TERM);
    assert_eq!(core.voted_for(), Some(NODE_ID));

    core.transition_to_candidate();

    assert_eq!(core.state(), NodeState::Leader);
    assert_eq!(core.current_term(), TERM);
    assert_eq!(core.voted_for(), Some(NODE_ID));
}

#[test]
fn test_core_transition_to_follower_and_reset_voted_for() {
    const TERM_0: u64 = 0;
    const TERM_1: u64 = 1;
    const TERM_2: u64 = 2;

    let mut core = NodeCore::new(NODE_ID);

    // check default values
    assert_eq!(core.state(), NodeState::Follower);
    assert_eq!(core.current_term(), TERM_0);
    assert_eq!(core.voted_for(), None);

    core.transition_to_candidate(); // sets term to 1, votes for self

    // check values after transition
    assert_eq!(core.state(), NodeState::Candidate);
    assert_eq!(core.current_term(), TERM_1);
    assert_eq!(core.voted_for(), Some(NODE_ID));

    core.transition_to_follower(TERM_2);

    assert_eq!(core.state(), NodeState::Follower);
    assert_eq!(core.current_term(), TERM_2);
    assert_eq!(core.voted_for(), None);
    assert_eq!(core.votes_received(), 0);
}

#[test]
fn test_core_transition_to_follower_does_not_reset_for_same_term() {
    const TERM_0: u64 = 0;

    let mut core = NodeCore::new(NODE_ID);

    // check default values
    assert_eq!(core.state(), NodeState::Follower);
    assert_eq!(core.current_term(), TERM_0);
    assert_eq!(core.voted_for(), None);

    core.transition_to_follower(TERM_0);

    // check values after transition
    assert_eq!(core.state(), NodeState::Follower);
    assert_eq!(core.current_term(), TERM_0);
    assert_eq!(core.voted_for(), None);
    assert_eq!(core.votes_received(), 0);
}

#[test]
fn test_core_transition_to_leader() {
    const TERM_0: u64 = 0;
    const TERM_1: u64 = 1;

    let mut core = NodeCore::new(NODE_ID);

    // start as follower
    assert_eq!(core.state(), NodeState::Follower);
    assert_eq!(core.current_term(), TERM_0);
    assert_eq!(core.voted_for(), None);

    // transition to candidate
    core.transition_to_candidate(); // sets term to 1, votes for self

    // transition to leader
    core.transition_to_leader(&[NODE_ID]);

    assert_eq!(core.state(), NodeState::Leader);
    assert_eq!(core.current_term(), TERM_1); // term should increase
    assert_eq!(core.voted_for(), Some(NODE_ID));
}

#[test]
fn test_core_transition_to_leader_does_not_transition_if_not_candidate() {
    const TERM: u64 = 0;

    let mut core = NodeCore::new(NODE_ID);

    // check default values
    assert_eq!(core.state(), NodeState::Follower);
    assert_eq!(core.current_term(), TERM);
    assert_eq!(core.voted_for(), None);

    core.transition_to_leader(&[NODE_ID]);

    assert_eq!(core.state(), NodeState::Follower);
    assert_eq!(core.current_term(), TERM);
    assert_eq!(core.voted_for(), None);
}

#[test]
fn test_core_update_term_success() {
    const TERM_0: u64 = 0;
    const TERM_1: u64 = 1;

    let mut core = NodeCore::new(NODE_ID);

    // check default values
    assert_eq!(core.current_term(), TERM_0);
    assert_eq!(core.voted_for(), None);

    // update term to 1
    let term_updated = core.update_term(TERM_1);
    assert!(term_updated);
    assert_eq!(core.current_term(), TERM_1);
    assert_eq!(core.voted_for(), None);
}

#[test]
fn test_core_update_term_failure() {
    const TERM_0: u64 = 0;

    let mut core = NodeCore::new(NODE_ID);

    // check default values
    assert_eq!(core.current_term(), TERM_0);
    assert_eq!(core.voted_for(), None);

    // update term to 1
    let term_updated = core.update_term(TERM_0); // same term

    assert!(!term_updated);
    assert_eq!(core.current_term(), TERM_0);
    assert_eq!(core.voted_for(), None);
}

#[test]
fn test_core_set_last_applied_increases_greater_index() {
    let mut core = NodeCore::new(NODE_ID);

    // check default values
    assert_eq!(core.last_applied(), 0);
    assert_eq!(core.commit_index(), 0);

    // set commit index to 1 since last applied can't be greater than commit index
    core.commit_index = 1;

    // set last applied to 1
    core.set_last_applied(1);
    assert_eq!(core.last_applied(), 1);
}

#[test]
fn test_core_set_last_applied_does_not_increase() {
    let mut core = NodeCore::new(NODE_ID);

    // check default values
    assert_eq!(core.last_applied(), 0);
    assert_eq!(core.commit_index(), 0);

    // set last applied to 1
    core.set_last_applied(1);
    assert_eq!(core.last_applied(), 0);
}

#[test]
fn test_core_follower_update_commit_index_success() {
    const LEADER_COMMIT_INDEX: u64 = 1;

    let mut core = NodeCore::new(NODE_ID);

    // check default values
    assert_eq!(core.commit_index(), 0);

    // add log entry because commit index can't be greater than log length
    core.log.push(LogEntry::new(0, "test".to_string()));

    core.follower_update_commit_index(LEADER_COMMIT_INDEX);

    assert_eq!(core.commit_index(), LEADER_COMMIT_INDEX);
}

#[test]
fn test_core_follower_update_commit_index_failure() {
    const LEADER_COMMIT_INDEX: u64 = 1;
    const ORIGINAL_COMMIT_INDEX: u64 = 0;

    let mut core = NodeCore::new(NODE_ID);

    // check default values
    assert_eq!(core.commit_index(), ORIGINAL_COMMIT_INDEX);

    core.follower_update_commit_index(LEADER_COMMIT_INDEX);

    assert_eq!(core.commit_index(), ORIGINAL_COMMIT_INDEX);
}

#[test]
fn test_core_follower_append_entries() {
    let mut core = NodeCore::new(NODE_ID);
    const TERM: u64 = 1;

    // Create test entries
    let entries =
        vec![LogEntry::new(TERM, "test1".to_string()), LogEntry::new(TERM, "test2".to_string())];

    // Test appending entries
    let (consistent, modified) = core.follower_append_entries(0, TERM, &entries);
    assert!(consistent);
    assert!(modified);
    assert_eq!(core.log().len(), 2);
    assert_eq!(core.log()[0].term, TERM);
    assert_eq!(core.log()[1].term, TERM);
}

#[test]
fn test_core_follower_append_entries_empty() {
    let mut core = NodeCore::new(NODE_ID);

    // Test appending empty entries (heartbeat)
    let (consistent, modified) = core.follower_append_entries(0, 0, &[]);
    assert!(consistent);
    assert!(!modified);
    assert_eq!(core.log().len(), 0);
}

#[test]
fn test_core_decide_vote_newer_term() {
    let mut core = NodeCore::new(NODE_ID);
    const CANDIDATE_ID: u64 = 1;
    const CANDIDATE_TERM: u64 = 2;

    // Test voting for candidate with newer term
    let (granted, term) = core.decide_vote(CANDIDATE_ID, CANDIDATE_TERM, 0, 0);
    assert!(granted);
    assert_eq!(term, CANDIDATE_TERM);
    assert_eq!(core.current_term(), CANDIDATE_TERM);
    assert_eq!(core.voted_for(), Some(CANDIDATE_ID));
}

#[test]
fn test_core_decide_vote_older_term() {
    let mut core = NodeCore::new(NODE_ID);
    const CANDIDATE_ID: u64 = 1;
    const CANDIDATE_TERM: u64 = 0;
    const CURRENT_TERM: u64 = 1;

    // Set current term to 1
    core.current_term = CURRENT_TERM;

    // Test voting for candidate with older term
    let (granted, term) = core.decide_vote(CANDIDATE_ID, CANDIDATE_TERM, 0, 0);
    assert!(!granted); // Should not grant vote to candidate with older term
    assert_eq!(term, CURRENT_TERM); // Should keep current term
    assert_eq!(core.current_term(), CURRENT_TERM);
    assert_eq!(core.voted_for(), None); // Should not record vote
}

#[test]
fn test_core_decide_vote_already_voted() {
    let mut core = NodeCore::new(NODE_ID);
    const CANDIDATE_ID: u64 = 1;
    const OTHER_CANDIDATE_ID: u64 = 2;
    const TERM: u64 = 1;

    // First vote
    let (granted, term) = core.decide_vote(CANDIDATE_ID, TERM, 0, 0);
    assert!(granted);
    assert_eq!(term, TERM);
    assert_eq!(core.voted_for(), Some(CANDIDATE_ID));

    // Second vote for different candidate
    let (granted, term) = core.decide_vote(OTHER_CANDIDATE_ID, TERM, 0, 0);
    assert!(!granted);
    assert_eq!(term, TERM);
    assert_eq!(core.voted_for(), Some(CANDIDATE_ID));
}

#[test]
fn test_core_record_vote_received() {
    let mut core = NodeCore::new(NODE_ID);

    // Setup: transition to candidate
    core.transition_to_candidate();
    assert_eq!(core.votes_received(), 1); // Self vote

    // Record additional vote
    core.record_vote_received();
    assert_eq!(core.votes_received(), 2);
}

#[test]
fn test_core_record_vote_received_not_candidate() {
    let mut core = NodeCore::new(NODE_ID);

    // Record vote while not in candidate state
    core.record_vote_received();
    assert_eq!(core.votes_received(), 0);
}

#[test]
fn test_core_leader_append_entry() {
    const TERM: u64 = 1;
    let mut core = setup_leader_core(NODE_ID, TERM, vec![], &[PEER_ID]);

    // Test successful append
    let success = core.leader_append_entry("test".to_string());
    assert!(success);
    assert_eq!(core.log().len(), 1);
    assert_eq!(core.log()[0].term, TERM);
}

#[test]
fn test_core_leader_append_entry_not_leader() {
    let mut core = NodeCore::new(NODE_ID);

    // Test append while not in leader state
    let success = core.leader_append_entry("test".to_string());
    assert!(!success);
    assert_eq!(core.log().len(), 0);
}

#[test]
fn test_core_leader_recalculate_commit_index_no_change() {
    let peers = [PEER_ID, PEER_ID_2, PEER_ID_3];
    let total_nodes = peers.len();
    let mut core = setup_leader_core(NODE_ID, 1, vec![], &peers);

    // Add log entries from current term
    core.leader_append_entry("test1".to_string());
    core.leader_append_entry("test2".to_string());

    // Set match indices for followers (not enough for majority)
    core.match_index.insert(PEER_ID, 0);
    core.match_index.insert(PEER_ID_2, 0);

    // Initial state
    assert_eq!(core.commit_index(), 0);
    assert_eq!(core.log_last_index(), 2);

    // Test recalculation with no change
    let changed = core.leader_recalculate_commit_index(total_nodes);
    assert!(!changed);
    assert_eq!(core.commit_index(), 0);
}

#[test]
fn test_core_leader_recalculate_commit_index_with_change() {
    let peers = [PEER_ID, PEER_ID_2, PEER_ID_3];
    let total_nodes = peers.len();
    let mut core = setup_leader_core(NODE_ID, 1, vec![], &peers);

    // Add log entries from current term
    core.leader_append_entry("test1".to_string());
    core.leader_append_entry("test2".to_string());

    // Set match indices for followers (enough for majority)
    core.match_index.insert(PEER_ID, 1);
    core.match_index.insert(PEER_ID_2, 1);

    // Initial state
    assert_eq!(core.commit_index(), 0);
    assert_eq!(core.log_last_index(), 2);

    // Test recalculation with change
    let changed = core.leader_recalculate_commit_index(total_nodes);
    assert!(changed);
    assert_eq!(core.commit_index(), 1);
}

#[test]
fn test_core_leader_recalculate_commit_index_with_previous_term_entries() {
    const PREVIOUS_TERM: u64 = 1;
    let peers = [PEER_ID, PEER_ID_2, PEER_ID_3];
    let total_nodes = peers.len();
    let mut core = setup_leader_core(NODE_ID, 1, vec![], &peers);

    // Add log entries from previous term
    core.log.push(LogEntry::new(PREVIOUS_TERM, "test1".to_string()));
    core.log.push(LogEntry::new(PREVIOUS_TERM, "test2".to_string()));

    // Add log entry from current term
    core.leader_append_entry("test3".to_string());

    // Set match indices for followers (enough for majority)
    core.match_index.insert(PEER_ID, 2);
    core.match_index.insert(PEER_ID_2, 2);

    // Initial state
    assert_eq!(core.commit_index(), 0);
    assert_eq!(core.log_last_index(), 3);

    // Test recalculation with change
    let changed = core.leader_recalculate_commit_index(total_nodes);
    assert!(changed);
    assert_eq!(core.commit_index(), 2);
}

#[test]
fn test_core_leader_recalculate_commit_index_with_mixed_term_entries() {
    const PREVIOUS_TERM: u64 = 1;
    let peers = [PEER_ID, PEER_ID_2, PEER_ID_3];
    let total_nodes = peers.len();
    let mut core = setup_leader_core(NODE_ID, 1, vec![], &peers);

    // Add log entries from previous term
    core.log.push(LogEntry::new(PREVIOUS_TERM, "test1".to_string()));
    core.log.push(LogEntry::new(PREVIOUS_TERM, "test2".to_string()));

    // Add log entries from current term
    core.leader_append_entry("test3".to_string());
    core.leader_append_entry("test4".to_string());

    // Set match indices for followers (enough for majority for first current term
    // entry)
    core.match_index.insert(PEER_ID, 3);
    core.match_index.insert(PEER_ID_2, 3);

    // Initial state
    assert_eq!(core.commit_index(), 0);
    assert_eq!(core.log_last_index(), 4);

    // Test recalculation with change
    let changed = core.leader_recalculate_commit_index(total_nodes);
    assert!(changed);
    assert_eq!(core.commit_index(), 3);
}

#[test]
fn test_core_leader_recalculate_commit_index_with_odd_number_of_nodes() {
    let peers = [PEER_ID, PEER_ID_2, PEER_ID_3, PEER_ID_4];
    let total_nodes = peers.len();
    let mut core = setup_leader_core(NODE_ID, 1, vec![], &peers);

    // Add log entries from current term
    core.leader_append_entry("test1".to_string());
    core.leader_append_entry("test2".to_string());

    // Set match indices for followers (exactly majority)
    // We need 3 nodes (including leader) to have the entry at index 1
    // Leader has all entries, so we need 2 more followers
    core.match_index.insert(PEER_ID, 1);
    core.match_index.insert(PEER_ID_2, 1);
    core.match_index.insert(PEER_ID_3, 0);
    core.match_index.insert(PEER_ID_4, 0);

    // Initial state
    assert_eq!(core.commit_index(), 0);
    assert_eq!(core.log_last_index(), 2);

    // Test recalculation with change
    let changed = core.leader_recalculate_commit_index(total_nodes);
    assert!(changed);
    assert_eq!(core.commit_index(), 1);
}

#[test]
fn test_core_leader_recalculate_commit_index_with_even_number_of_nodes() {
    let peers = [PEER_ID, PEER_ID_2, PEER_ID_3, PEER_ID_4];
    let total_nodes = peers.len();
    let mut core = setup_leader_core(NODE_ID, 1, vec![], &peers);

    // Add log entries from current term
    core.leader_append_entry("test1".to_string());
    core.leader_append_entry("test2".to_string());

    // Set match indices for followers (exactly majority)
    // We need 3 nodes (including leader) to have the entry at index 1
    // Leader has all entries, so we need 2 more followers
    core.match_index.insert(PEER_ID, 1);
    core.match_index.insert(PEER_ID_2, 1);
    core.match_index.insert(PEER_ID_3, 0);

    // Initial state
    assert_eq!(core.commit_index(), 0);
    assert_eq!(core.log_last_index(), 2);

    // Test recalculation with change
    let changed = core.leader_recalculate_commit_index(total_nodes);
    assert!(changed);
    assert_eq!(core.commit_index(), 1);
}

#[test]
fn test_core_leader_recalculate_commit_index_with_minimal_majority() {
    let peers = [NODE_ID, PEER_ID, PEER_ID_2];
    let total_nodes = peers.len();
    let mut core = setup_leader_core(NODE_ID, 1, vec![], &peers);

    // Add log entries from current term
    core.leader_append_entry("test1".to_string());

    // Set match indices for followers (exactly majority)
    // We need 2 nodes (including leader) to have the entry at index 1
    // Leader has all entries, so we need 1 more follower
    core.match_index.insert(PEER_ID, 1);
    core.match_index.insert(PEER_ID_2, 0);

    // Initial state
    assert_eq!(core.commit_index(), 0);
    assert_eq!(core.log_last_index(), 1);

    // Test recalculation with change
    let changed = core.leader_recalculate_commit_index(total_nodes);
    assert!(changed);
    assert_eq!(core.commit_index(), 1);
}

#[test]
fn test_core_leader_process_response_not_leader() {
    let mut core = NodeCore::new(NODE_ID);
    let peers = [NODE_ID, PEER_ID];
    let total_nodes = peers.len();

    // Add some state that shouldn't change
    core.commit_index = 1;
    core.next_index.insert(PEER_ID, 2);
    core.match_index.insert(PEER_ID, 1);

    // Action: Call function when not leader
    let (advanced, old_ci, new_ci) =
        core.leader_process_append_response(PEER_ID, true, 0, 1, total_nodes);

    // Assertions: No change
    assert!(!advanced);
    assert_eq!(old_ci, 1);
    assert_eq!(new_ci, 1);
    assert_eq!(core.commit_index(), 1);
    assert_eq!(core.next_index_for(PEER_ID), Some(2));
    assert_eq!(core.match_index_for(PEER_ID), Some(1));
}

#[test]
fn test_core_leader_process_response_success_no_commit() {
    let peers = [NODE_ID, PEER_ID, PEER_ID_2]; // 3 nodes, majority = 2
    let total_nodes = peers.len();
    let log = vec![LogEntry::new(1, "cmd1".into()), LogEntry::new(1, "cmd2".into())];
    let mut core = setup_leader_core(NODE_ID, 1, log, &peers);

    // Initial state: commit=0, match[1]=0, next[1]=3, match[2]=0, next[2]=3
    assert_eq!(core.commit_index(), 0);
    assert_eq!(core.match_index_for(PEER_ID), Some(0));
    assert_eq!(core.next_index_for(PEER_ID), Some(3));
    assert_eq!(core.match_index_for(PEER_ID_2), Some(0));
    assert_eq!(core.next_index_for(PEER_ID_2), Some(3));

    // Action: PEER_ID successfully appends entries up to index 2 (sent
    // prev=0, len=2)
    let (advanced, old_ci, new_ci) =
        core.leader_process_append_response(PEER_ID, true, 0, 2, total_nodes);

    // Assertions:
    // After PEER_ID matches index 2, the leader (match=2) and PEER_ID
    // (match=2) form a majority for index 1 and 2. Both entries are term 1
    // (current term). Commit should advance to 2.
    assert!(advanced); // Commit index should advance
    assert_eq!(old_ci, 0);
    assert_eq!(new_ci, 2);
    assert_eq!(core.commit_index(), 2);
    assert_eq!(core.match_index_for(PEER_ID), Some(2)); // Updated
    assert_eq!(core.next_index_for(PEER_ID), Some(3)); // Updated (match + 1)
    assert_eq!(core.match_index_for(PEER_ID_2), Some(0)); // Unchanged
    assert_eq!(core.next_index_for(PEER_ID_2), Some(3)); // Unchanged
}

#[test]
fn test_core_leader_process_response_success_triggers_commit() {
    let peers = [NODE_ID, PEER_ID, PEER_ID_2]; // 3 nodes, majority = 2
    let total_nodes = peers.len();
    let log = vec![LogEntry::new(1, "cmd1".into()), LogEntry::new(1, "cmd2".into())];
    let mut core = setup_leader_core(NODE_ID, 1, log, &peers);
    core.match_index.insert(PEER_ID, 1); // Simulate PEER_ID already acked entry 1
    core.next_index.insert(PEER_ID, 2);

    // Initial state: commit=0, match[1]=1, next[1]=2, match[2]=0, next[2]=3
    assert_eq!(core.commit_index(), 0);

    // Action: PEER_2 successfully appends entries up to index 1 (sent prev=0,
    // len=1)
    let (advanced, old_ci, new_ci) =
        core.leader_process_append_response(PEER_ID_2, true, 0, 1, total_nodes);

    // Assertions:
    // Now leader (match=2), PEER_ID (match=1), PEER_ID_2 (match=1).
    // Majority (2) >= index 1 is met. Entry 1 is term 1 (current). Commit should
    // advance.
    assert!(advanced);
    assert_eq!(old_ci, 0);
    assert_eq!(new_ci, 1);
    assert_eq!(core.commit_index(), 1);
    assert_eq!(core.match_index_for(PEER_ID), Some(1)); // Unchanged by this response
    assert_eq!(core.next_index_for(PEER_ID), Some(2)); // Unchanged by this response
    assert_eq!(core.match_index_for(PEER_ID_2), Some(1)); // Updated
    assert_eq!(core.next_index_for(PEER_ID_2), Some(2)); // Updated (match + 1)
}

#[test]
fn test_core_leader_process_response_failure_decrements_next() {
    let peers = [NODE_ID, PEER_ID, PEER_ID_2];
    let total_nodes = peers.len();
    let log = vec![LogEntry::new(1, "cmd1".into()), LogEntry::new(1, "cmd2".into())];
    let mut core = setup_leader_core(NODE_ID, 1, log, &peers);

    // Initial state: commit=0, match[1]=0, next[1]=3
    let initial_next = core.next_index_for(PEER_ID).unwrap();
    assert_eq!(initial_next, 3);
    assert_eq!(core.commit_index(), 0);
    assert_eq!(core.match_index_for(PEER_ID), Some(0));

    // Action: PEER_ID fails to append
    let (advanced, old_ci, new_ci) =
        core.leader_process_append_response(PEER_ID, false, 1, 1, total_nodes);

    // Assertions:
    assert!(!advanced);
    assert_eq!(old_ci, 0);
    assert_eq!(new_ci, 0);
    assert_eq!(core.commit_index(), 0); // No change
    assert_eq!(core.match_index_for(PEER_ID), Some(0)); // No change
    assert_eq!(core.next_index_for(PEER_ID), Some(initial_next - 1)); // Decremented
    assert_eq!(core.next_index_for(PEER_ID), Some(2));
}

#[test]
fn test_core_leader_process_response_failure_next_already_1() {
    let peers = [NODE_ID, PEER_ID];
    let total_nodes = peers.len();
    let log = vec![LogEntry::new(1, "cmd1".into())];
    let mut core = setup_leader_core(NODE_ID, 1, log, &peers);
    core.next_index.insert(PEER_ID, 1); // Set next index to 1

    // Initial state: commit=0, match[1]=0, next[1]=1
    assert_eq!(core.next_index_for(PEER_ID), Some(1));
    assert_eq!(core.commit_index(), 0);
    assert_eq!(core.match_index_for(PEER_ID), Some(0));

    // Action: PEER_ID fails to append
    let (advanced, old_ci, new_ci) =
        core.leader_process_append_response(PEER_ID, false, 0, 0, total_nodes);

    // Assertions:
    assert!(!advanced);
    assert_eq!(old_ci, 0);
    assert_eq!(new_ci, 0);
    assert_eq!(core.commit_index(), 0); // No change
    assert_eq!(core.match_index_for(PEER_ID), Some(0)); // No change
    assert_eq!(core.next_index_for(PEER_ID), Some(1)); // Still 1, not decremented below 1
}

#[test]
fn test_core_leader_process_response_success_stale() {
    // Test receiving a success response for entries already matched or exceeded.
    let peers = [NODE_ID, PEER_ID];
    let total_nodes = peers.len();
    let log = vec![LogEntry::new(1, "cmd1".into()), LogEntry::new(1, "cmd2".into())];
    let mut core = setup_leader_core(NODE_ID, 1, log, &peers);
    core.match_index.insert(PEER_ID, 2); // Peer already matches index 2
    core.next_index.insert(PEER_ID, 3);

    // Initial state: commit=0, match[1]=2, next[1]=3
    assert_eq!(core.match_index_for(PEER_ID), Some(2));
    assert_eq!(core.next_index_for(PEER_ID), Some(3));
    assert_eq!(core.commit_index(), 0);

    // Action: Receive a success response for having appended up to index 1 (sent
    // prev=0, len=1) This is "stale" because we already know matchIndex is 2.
    let (advanced, old_ci, new_ci) =
        core.leader_process_append_response(PEER_ID, true, 0, 1, total_nodes);

    // Assertions: Indices should not decrease, commit shouldn't change based on
    // this stale info
    assert!(!advanced); // No commit advancement triggered by stale info
    assert_eq!(old_ci, 0);
    assert_eq!(new_ci, 0); // Commit index remains 0
    assert_eq!(core.commit_index(), 0);
    assert_eq!(core.match_index_for(PEER_ID), Some(2)); // Match index did not decrease
    assert_eq!(core.next_index_for(PEER_ID), Some(3)); // Next index reflects current match
}

#[test]
fn test_core_find_conflicts_and_append_empty_log() {
    let mut core = NodeCore::new(NODE_ID);
    let entries = vec![LogEntry::new(1, "cmd1".into()), LogEntry::new(1, "cmd2".into())];

    // Test appending to an empty log
    let modified = core.find_conflicts_and_append(0, &entries);

    assert!(modified);
    assert_eq!(core.log().len(), 2);
    assert_eq!(core.log()[0].term, 1);
    assert_eq!(core.log()[0].command, "cmd1");
    assert_eq!(core.log()[1].term, 1);
    assert_eq!(core.log()[1].command, "cmd2");
}

#[test]
fn test_core_find_conflicts_and_append_no_conflict() {
    let mut core = NodeCore::new(NODE_ID);
    // Add initial entries
    core.log.push(LogEntry::new(1, "cmd1".into()));
    core.log.push(LogEntry::new(1, "cmd2".into()));

    // New entries that don't conflict
    let entries = vec![LogEntry::new(1, "cmd3".into()), LogEntry::new(1, "cmd4".into())];

    // Test appending with no conflict
    let modified = core.find_conflicts_and_append(2, &entries);

    assert!(modified);
    assert_eq!(core.log_last_index(), 4);
    assert_eq!(core.log()[2].term, 1);
    assert_eq!(core.log()[2].command, "cmd3");
    assert_eq!(core.log()[3].term, 1);
    assert_eq!(core.log()[3].command, "cmd4");
}

#[test]
fn test_core_find_conflicts_and_append_with_conflict() {
    let mut core = NodeCore::new(NODE_ID);
    // Add initial entries
    core.log.push(LogEntry::new(1, "cmd1".into()));
    core.log.push(LogEntry::new(1, "cmd2".into()));
    core.log.push(LogEntry::new(2, "cmd3".into())); // Different term

    // New entries that conflict with existing entries
    let entries = vec![LogEntry::new(1, "cmd4".into()), LogEntry::new(1, "cmd5".into())];

    // Test appending with conflict
    let modified = core.find_conflicts_and_append(1, &entries);

    assert!(modified);
    assert_eq!(core.log_last_index(), 3);
    assert_eq!(core.log()[0].term, 1);
    assert_eq!(core.log()[0].command, "cmd1");
    assert_eq!(core.log()[1].term, 1);
    assert_eq!(core.log()[1].command, "cmd2");
    assert_eq!(core.log()[2].term, 1);
    assert_eq!(core.log()[2].command, "cmd5");
}

#[test]
fn test_core_find_conflicts_and_append_no_change() {
    let mut core = NodeCore::new(NODE_ID);
    // Add initial entries
    core.log.push(LogEntry::new(1, "cmd1".into()));
    core.log.push(LogEntry::new(1, "cmd2".into()));

    // New entries that match existing entries
    let entries = vec![LogEntry::new(1, "cmd2".into())];

    // Test appending with no change (entries already exist)
    let modified = core.find_conflicts_and_append(1, &entries);

    assert!(!modified);
    assert_eq!(core.log_last_index(), 2);
    assert_eq!(core.log()[0].term, 1);
    assert_eq!(core.log()[0].command, "cmd1");
    assert_eq!(core.log()[1].term, 1);
    assert_eq!(core.log()[1].command, "cmd2");
}

#[test]
fn test_core_find_conflicts_and_append_beyond_log() {
    let mut core = NodeCore::new(NODE_ID);
    // Add initial entries
    core.log.push(LogEntry::new(1, "cmd1".into()));
    core.log.push(LogEntry::new(1, "cmd2".into()));

    // New entries that go beyond the current log
    let entries = vec![LogEntry::new(1, "cmd3".into()), LogEntry::new(1, "cmd4".into())];

    // Test appending beyond the current log
    let modified = core.find_conflicts_and_append(3, &entries);

    assert!(modified);
    assert_eq!(core.log_last_index(), 4);
    assert_eq!(core.log()[2].term, 1);
    assert_eq!(core.log()[2].command, "cmd3");
    assert_eq!(core.log()[3].term, 1);
    assert_eq!(core.log()[3].command, "cmd4");
}
