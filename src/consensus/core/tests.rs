use crate::consensus::{
    LogEntry,
    core::{NodeCore, NodeState},
};

const NODE_ID: u64 = 0;

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
fn test_core_leader_update_commit_index_success() {
    let mut core = NodeCore::new(NODE_ID);
    const FOLLOWER_ID: u64 = 1;

    // Setup: transition to leader and add some log entries
    core.transition_to_candidate();
    core.transition_to_leader(&[NODE_ID]);
    core.leader_append_entry("test1".to_string());
    core.leader_append_entry("test2".to_string());

    // Initial state
    assert_eq!(core.commit_index(), 0);
    assert_eq!(core.log_last_index(), 2);

    // Test successful update
    let result = core.leader_update_commit_index(FOLLOWER_ID);
    assert_eq!(result, Some((0, 2)));
    assert_eq!(core.commit_index(), 2);
}

#[test]
fn test_core_leader_update_commit_index_no_change() {
    let mut core = NodeCore::new(NODE_ID);
    const FOLLOWER_ID: u64 = 1;

    // Setup: transition to leader and add some log entries
    core.transition_to_candidate();
    core.transition_to_leader(&[NODE_ID]);
    core.leader_append_entry("test1".to_string());
    core.leader_append_entry("test2".to_string());
    core.commit_index = 2; // Set commit index to log length

    // Test update when commit index is already at log length
    let result = core.leader_update_commit_index(FOLLOWER_ID);
    assert_eq!(result, None);
    assert_eq!(core.commit_index(), 2);
}

#[test]
#[ignore = "Currently never fails, update is optimistic"]
fn test_core_leader_update_commit_index_failure() {
    let mut core = NodeCore::new(NODE_ID);
    const FOLLOWER_ID: u64 = 1;

    // Setup: transition to leader and add some log entries
    core.transition_to_candidate();
    core.transition_to_leader(&[NODE_ID]);
    core.leader_append_entry("test1".to_string());
    core.leader_append_entry("test2".to_string());

    // Test failed append entries
    let result = core.leader_update_commit_index(FOLLOWER_ID);
    assert_eq!(result, None);
    assert_eq!(core.commit_index(), 0);
}

#[test]
fn test_core_leader_update_commit_index_not_leader() {
    let mut core = NodeCore::new(NODE_ID);
    const FOLLOWER_ID: u64 = 1;

    // Test when not in leader state
    let result = core.leader_update_commit_index(FOLLOWER_ID);
    assert_eq!(result, None);
    assert_eq!(core.commit_index(), 0);
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
    let mut core = NodeCore::new(NODE_ID);
    const TERM: u64 = 1;

    // Setup: transition to leader
    core.transition_to_candidate();
    core.transition_to_leader(&[NODE_ID]);

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
