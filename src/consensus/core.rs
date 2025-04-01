use std::{cmp::Ordering, collections::HashMap};

use log::{debug, error, info, warn};

use crate::consensus::LogEntry;

/// The state of a node in the consensus protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NodeState {
    #[default]
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Default)]
pub struct NodeCore {
    /// The id of the node.
    id: u64,

    // TODO: add to persistent storage
    /// The current term of the node.
    current_term: u64,
    /// The node that the node has voted for.
    voted_for: Option<u64>,
    /// The log of the node.
    log: Vec<LogEntry>,

    // Volatile state
    /// The commit index of the node.
    commit_index: u64,
    /// The last applied index of the node.
    last_applied: u64,
    /// The state of the node.
    state: NodeState,

    // Candidate only
    /// The number of votes received by the node.
    votes_received: u64,

    // Leader only
    /// The next index of the node for each node.
    next_index: HashMap<u64, u64>,
}

// Constructors
impl NodeCore {
    pub fn new(id: u64) -> Self {
        Self { id, ..Default::default() }
    }
}

// Getters
impl NodeCore {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn state(&self) -> NodeState {
        self.state
    }

    pub fn current_term(&self) -> u64 {
        self.current_term
    }

    pub fn voted_for(&self) -> Option<u64> {
        self.voted_for
    }

    pub fn log(&self) -> &[LogEntry] {
        &self.log
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    pub fn votes_received(&self) -> u64 {
        self.votes_received
    }

    pub fn last_applied(&self) -> u64 {
        self.last_applied
    }

    /// Get the last index of the log (1-based).
    pub fn log_last_index(&self) -> u64 {
        self.log.len() as u64
    }
}

// Setters
impl NodeCore {
    /// Set the last applied index.
    pub fn set_last_applied(&mut self, index: u64) {
        // Ensure last_applied is not greater than commit_index
        let applied_index = index.min(self.commit_index());

        match applied_index.cmp(&self.last_applied) {
            Ordering::Greater => {
                self.last_applied = applied_index;
                debug!("Node {} updated last_applied to {}", self.id, self.last_applied);
            }
            Ordering::Less => {
                error!(
                    "Node {} attempted to set last_applied to {} (lower than current {})",
                    self.id, applied_index, self.last_applied
                );
            }
            Ordering::Equal => {
                debug!("Node {} last_applied is already {}", self.id, self.last_applied);
            }
        }
    }

    /// Update the term of the node and reset the vote if new_term is greater
    /// than current_term. Returns true if the term was updated, false
    /// otherwise.
    fn update_term(&mut self, new_term: u64) -> bool {
        if new_term > self.current_term() {
            info!("Node {} updated term from {} to {}", self.id, self.current_term, new_term);
            self.current_term = new_term;
            self.voted_for = None;
            true
        } else {
            false
        }
    }
}

// State transitions
impl NodeCore {
    /// Transition to a follower and reset votes.
    pub fn transition_to_follower(&mut self, term: u64) {
        let term_updated = self.update_term(term);
        let state_changed = self.state() != NodeState::Follower;

        if term_updated || state_changed {
            info!("Node {} transitioning to follower state at term {}", self.id, term);
            self.state = NodeState::Follower;
            self.votes_received = 0; // reset votes
        }
    }

    /// Transition to a candidate and vote for self.
    pub fn transition_to_candidate(&mut self) {
        if self.state() == NodeState::Leader {
            warn!("Node {} attempted to transition to candidate state but is a Leader", self.id);
            return;
        }

        let new_term = self.current_term() + 1;
        info!("Node {} transitioning to candidate state at term {}", self.id, new_term);

        let term_updated = self.update_term(new_term);
        assert!(term_updated, "Term should increase");
        self.state = NodeState::Candidate;
        self.voted_for = Some(self.id);
        self.votes_received = 1; // add self vote
    }

    /// Transition to a leader.
    pub fn transition_to_leader(&mut self) {
        if self.state() != NodeState::Candidate {
            warn!(
                "Node {} attempted to transition to leader state but is not a candidate",
                self.id
            );
            return;
        }

        info!("Node {} transitioning to leader state at term {}", self.id, self.current_term());
        self.state = NodeState::Leader;
        self.votes_received = 0; // reset votes

        // TODO: initialize Leader only state
    }

    /// Update the commit index.
    pub fn follower_update_commit_index(&mut self, leader_commit_index: u64) {
        let old_commit_index = self.commit_index();
        let follower_commit_limit = self.log().len() as u64;
        if leader_commit_index > old_commit_index {
            self.commit_index = leader_commit_index.min(follower_commit_limit);
            if self.commit_index() > old_commit_index {
                // Log only if changed
                info!(
                    "Node {} updated commit_index from {} to {} (leader_commit: {})",
                    self.id,
                    old_commit_index,
                    self.commit_index(),
                    leader_commit_index
                );
            } else {
                // Log if leader's commit is ahead but capped by our log length
                debug!(
                    "Node {} commit_index update capped at {} (leader_commit: {})",
                    self.id,
                    self.commit_index(),
                    leader_commit_index
                );
            }
        }
    }

    /// Append a new entry to the leader's log.
    pub fn leader_append_entry(&mut self, command: String) -> bool {
        if self.state != NodeState::Leader {
            warn!("Node {} tried to append entry but is not a Leader", self.id);
            return false;
        }
        let entry = LogEntry::new(self.current_term(), command);
        info!("Leader Node {} appending new log entry: {:?}", self.id, entry);
        self.log.push(entry);
        true
    }

    /// Update the commit index of the leader.
    pub fn leader_update_commit_index(
        &mut self,
        from_id: u64,
        success: bool,
        __total_nodes: u64,
    ) -> Option<(u64, u64)> {
        if self.state != NodeState::Leader {
            warn!("Node {} tried to update commit index but is not a Leader", self.id);
            return None;
        }

        if success {
            let potential_commit_index = self.log_last_index(); // Commit everything in log

            if potential_commit_index > self.commit_index {
                let old_commit = self.commit_index;
                info!(
                    "Node {} (Leader) OPTIMISTICALLY advancing commit index from {} to {} after \
                     success from {}",
                    self.id, old_commit, potential_commit_index, from_id
                );
                self.commit_index = potential_commit_index;
                return Some((old_commit, self.commit_index)); // Indicate change
            } else {
                debug!(
                    "Node {} (Leader) commit index ({}) already >= log length ({}), no change.",
                    self.id, self.commit_index, potential_commit_index
                );
            }
        } else {
            // Append failed on follower
            warn!(
                "Node {} (Leader): Follower {} failed AppendEntries (nextIndex handling TBD)",
                self.id, from_id
            );
            // Still attempt basic nextIndex decrement (placeholder)
            if let Some(idx) = self.next_index.get_mut(&from_id) {
                if *idx > 1 {
                    *idx -= 1;
                    info!(
                        "Node {} (Leader) decremented next_index for {} to {}",
                        self.id, from_id, *idx
                    );
                    // TODO: Need mechanism to trigger resend
                }
            }
        }

        None // Commit index did not change (or failure occurred)
    }

    /// Check if the log is consistent with another log.
    // TODO: Implement this.
    fn check_log_consistency(&self) -> bool {
        true
    }

    // Handles appending entries received from a leader
    // Returns `(log_consistent_and_appended, log_was_modified)` flags.
    pub fn follower_append_entries(&mut self, entries: &[LogEntry]) -> (bool, bool) {
        // 1. Perform Log Consistency Check
        // TODO: Implement real check
        if !self.check_log_consistency() {
            warn!("Log consistency check failed (TBD).");
            return (false, false); // Return false for consistency check failure
        }

        // 2. Handle potential conflicts and append new entries
        // TODO: Implement conflict detection and log truncation
        let log_modified = self.find_conflicts_and_append(entries);

        // Return true for consistency (as check passed), and whether log was modified
        (true, log_modified)
    }

    /// Finds conflicting entries, appends new entries.
    /// Returns true if the log was modified.
    fn find_conflicts_and_append(&mut self, entries: &[LogEntry]) -> bool {
        if entries.is_empty() {
            debug!("Node {} received heartbeat (no entries to append/check)", self.id());
            return false;
        }

        // TODO: Implement real conflict detection
        info!("Node {} appending {} entries (simplistic)", self.id(), entries.len());
        self.log.extend_from_slice(entries);
        true
    }

    // Voting methods
    /// Decides whether to grant vote based on RequestVote RPC args.
    /// Updates term and voted_for state internally if appropriate.
    /// Returns `(should_grant_vote, term_to_respond_with)`
    pub fn decide_vote(&mut self, candidate_id: u64, candidate_term: u64) -> (bool, u64) {
        // 1. If candidate_term is older than current_term, reject
        if candidate_term < self.current_term() {
            return (false, self.current_term());
        }
        // 2. If candidate_term is greater than current_term, convert to follower and
        //    reset voted_for
        if candidate_term > self.current_term() {
            self.transition_to_follower(candidate_term);
        }

        // TODO: check candidate log is consistent

        // 3. Vote if we haven't voted yet.
        let (vote_granted, term_to_respond_with) = self.grant_vote_if_possible(candidate_id);
        if vote_granted {
            info!(
                "Node {} voted for candidate {} in term {}",
                self.id, candidate_id, candidate_term
            );
            (true, term_to_respond_with)
        } else {
            debug!(
                "Node {} did not vote for candidate {} in term {}",
                self.id, candidate_id, candidate_term
            );

            (false, term_to_respond_with)
        }
    }

    /// Record a vote for a candidate if not already voted for someone else.
    /// Returns `(vote_granted, term_to_respond_with)`
    fn grant_vote_if_possible(&mut self, candidate_id: u64) -> (bool, u64) {
        match self.voted_for() {
            Some(voted_for) => (voted_for == candidate_id, self.current_term()),
            None => {
                info!("Node {} voting for candidate {}", self.id, candidate_id);
                self.voted_for = Some(candidate_id);
                (true, self.current_term())
            }
        }
    }

    /// Record a vote for self during candidate state.
    pub fn record_vote_received(&mut self) {
        if self.state() != NodeState::Candidate {
            warn!("Node {} attempted to record vote for self but is not a candidate", self.id);
        }

        self.votes_received += 1;

        info!(
            "Node {} received vote from Node {}, total votes: {}",
            self.id,
            self.id,
            self.votes_received()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
