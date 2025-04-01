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
    core.transition_to_leader();

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
    core.transition_to_leader();

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

    core.transition_to_leader();

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
