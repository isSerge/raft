#[cfg(test)]
mod tests;

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
    /// The match index of the node for each node.
    match_index: HashMap<u64, u64>,
}

// Constructors
impl NodeCore {
    pub fn new(id: u64) -> Self {
        Self { id, ..Default::default() }
    }

    // TODO: add constructors for testing with initial state
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

    /// Get the last term of the log.
    pub fn log_last_term(&self) -> u64 {
        // Return term 0 if log is empty, consistent with Raft's index 0.
        self.log.last().map_or(0, |entry| entry.term)
    }

    /// Get the next index for a node.
    pub fn next_index_for(&self, follower_id: u64) -> Option<u64> {
        self.next_index.get(&follower_id).copied()
    }

    /// Get the match index for a node.
    pub fn match_index_for(&self, follower_id: u64) -> Option<u64> {
        self.match_index.get(&follower_id).copied()
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
            Ordering::Equal => {}
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
    pub fn transition_to_leader(&mut self, peer_ids: &[u64]) {
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

        self.initialize_leader_state(peer_ids);
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

    fn initialize_leader_state(&mut self, peer_ids: &[u64]) {
        self.next_index.clear();
        self.match_index.clear();
        let last_log_index = self.log_last_index();

        for peer_id in peer_ids {
            if *peer_id == self.id {
                continue;
            }

            // + 1 because Raft uses 1-based indexing
            self.next_index.insert(*peer_id, last_log_index + 1);
            self.match_index.insert(*peer_id, 0);
        }

        info!(
            "Node {} initialized leader state with next_index: {:?}, match_index: {:?}",
            self.id, self.next_index, self.match_index
        );
    }

    /// Append a new entry to the leader's log.
    pub fn leader_append_entry(&mut self, command: String) -> bool {
        if self.state != NodeState::Leader {
            warn!("Node {} tried to append entry but is not a Leader", self.id);
            return false;
        }
        let entry = LogEntry::new(self.current_term(), command);
        info!(
            "Leader Node {} appending new log entry at index {}: {:?}",
            self.id,
            self.log_last_index() + 1,
            entry
        );
        self.log.push(entry);
        true
    }

    /// Process the response from a follower after appending entries.
    /// Returns `(commit_has_advanced, old_commit_index, new_commit_index)`
    pub fn leader_process_append_response(
        &mut self,
        from_id: u64,
        success: bool,
        prev_log_index: u64,
        entries_len: usize,
    ) -> (bool, u64, u64) {
        let old_commit_index = self.commit_index();

        if self.state != NodeState::Leader {
            warn!(
                "Node {} tried to process append response but is not a Leader. Ignoring.",
                self.id
            );
            return (false, old_commit_index, old_commit_index);
        }

        if success {
            let new_match_index = prev_log_index + entries_len as u64;
            let new_next_index = prev_log_index + 1;

            let current_match_index = self.match_index.entry(from_id).or_insert(0);

            if new_match_index > *current_match_index {
                *current_match_index = new_match_index;

                debug!(
                    "Node {} (Leader) updated match_index for {} from {} to {}",
                    self.id, from_id, *current_match_index, new_match_index
                );
            }

            self.next_index.insert(from_id, new_next_index);

            debug!(
                "Node {} (Leader) updated next_index for {} to {}",
                self.id, from_id, new_next_index
            );

            // TODO: update commit index based on majority of match_index
        } else {
            // Append failed. Decrement next_index for the follower.
            let current_next_index = self.next_index.entry(from_id).or_insert(0);

            if *current_next_index > 0 {
                *current_next_index -= 1;
                info!(
                    "Node {} (Leader) decremented next_index for {} to {}",
                    self.id, from_id, *current_next_index
                );
                // TODO: retry sending entries to the follower
            } else {
                warn!(
                    "Node {} (Leader) next_index for {} is 0. Cannot decrement further.",
                    self.id, from_id
                );
            }
        }

        let commit_has_advanced = self.commit_index() > old_commit_index;

        (commit_has_advanced, old_commit_index, self.commit_index())
    }

    /// Update the match index of the leader.
    pub fn leader_update_match_index(
        &mut self,
        from_id: u64,
        // success: bool,
        // total_nodes: u64,
        // Index of the last entry appended to the follower's log
        follower_last_append_index: u64,
    ) {
        if self.state != NodeState::Leader {
            warn!("Node {} tried to update match index but is not a Leader", self.id);
            return;
        }

        // Update match index
        let current_match_index = self.match_index.entry(from_id).or_insert(0);

        if follower_last_append_index > *current_match_index {
            *current_match_index = follower_last_append_index;

            debug!(
                "Node {} (Leader) updated match_index for {} from {} to {}",
                self.id, from_id, *current_match_index, follower_last_append_index
            );

            // TODO: recalculate leader's commit index
        }
    }

    /// Update the commit index of the leader.
    // TODO: should calculate based on majority of match_index
    pub fn leader_update_commit_index(&mut self, from_id: u64) -> Option<(u64, u64)> {
        let potential_commit_index = self.log_last_index();

        if potential_commit_index > self.commit_index() {
            if let Some(entry_to_commit) = self.log.get((potential_commit_index - 1) as usize) {
                if entry_to_commit.term == self.current_term() {
                    let old_commit = self.commit_index();
                    info!(
                        "Node {} (Leader) updated commit_index from {} to {} (from {})",
                        self.id, old_commit, potential_commit_index, from_id
                    );
                    self.commit_index = potential_commit_index;

                    Some((old_commit, self.commit_index()))
                } else {
                    warn!(
                        "Node {} (Leader) attempted to update commit_index but found conflicting \
                         term",
                        self.id
                    );
                    None
                }
            } else {
                warn!(
                    "Node {} (Leader) attempted to update commit_index but found no matching \
                     entry to commit",
                    self.id
                );
                None
            }
        } else {
            debug!(
                "Node {} (Leader) commit_index is up to date at {}",
                self.id,
                self.commit_index()
            );
            None
        }
    }

    /// Check if the log is consistent with another log.
    // TODO: Implement this.
    fn check_log_consistency(&self, prev_log_index: u64, prev_log_term: u64) -> bool {
        true
    }

    // Handles appending entries received from a leader
    // Returns `(log_consistent_and_appended, log_was_modified)` flags.
    pub fn follower_append_entries(
        &mut self,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: &[LogEntry],
    ) -> (bool, bool) {
        // 1. Perform Log Consistency Check
        let log_is_consistent = self.check_log_consistency(prev_log_index, prev_log_term);

        if !log_is_consistent {
            warn!(
                "Node {} (Follower) rejected append request (prev_log_index: {}, prev_log_term: \
                 {})",
                self.id, prev_log_index, prev_log_term
            );
            return (false, false);
        }

        // 2. Handle potential conflicts and append new entries
        // TODO: Implement conflict detection and log truncation
        let log_modified = self.find_conflicts_and_append(prev_log_index, entries);

        // Return true for consistency (as check passed), and whether log was modified
        (true, log_modified)
    }

    /// Finds conflicting entries, appends new entries.
    /// Returns true if the log was modified.
    fn find_conflicts_and_append(&mut self, prev_log_index: u64, entries: &[LogEntry]) -> bool {
        let mut log_modified = false;
        let mut leader_entry_index = 0;
        let mut current_raft_index = prev_log_index + 1;

        while leader_entry_index < entries.len() {
            let leader_entry = &entries[leader_entry_index];
            let current_vec_index = (current_raft_index - 1) as usize;

            if current_raft_index > self.log_last_index() {
                self.log.extend_from_slice(&entries[leader_entry_index..]);
                log_modified = true;
                break;
            }

            if let Some(follower_entry) = self.log.get(current_vec_index) {
                if follower_entry.term != leader_entry.term {
                    // Conflict detected. Truncate log.
                    self.log.truncate(current_vec_index);
                    log_modified = true;
                } else {
                    current_raft_index += 1;
                    leader_entry_index += 1;
                }
            } else {
                error!(
                    "Node {} logic error: follower index {} is within bounds but follower_entry \
                     is None",
                    self.id, current_raft_index
                );
                break;
            }
        }

        log_modified
    }

    // Voting methods
    /// Decides whether to grant vote based on RequestVote RPC args.
    /// Updates term and voted_for state internally if appropriate.
    /// Returns `(should_grant_vote, term_to_respond_with)`
    pub fn decide_vote(
        &mut self,
        candidate_id: u64,
        candidate_term: u64,
        candidate_last_log_index: u64,
        candidate_last_log_term: u64,
    ) -> (bool, u64) {
        // 1. If candidate_term is older than current_term, reject
        if candidate_term < self.current_term() {
            return (false, self.current_term());
        }
        // 2. If candidate_term is greater than current_term, convert to follower and
        //    reset voted_for
        if candidate_term > self.current_term() {
            self.transition_to_follower(candidate_term);
        }

        // 3. Check if already voted in this term (§5.2)
        let can_vote = match self.voted_for {
            // Already voted for the requesting candidate: grant again (idempotent)
            Some(id) if id == candidate_id => true,
            // Already voted for someone else: reject
            Some(_) => false,
            // Haven't voted yet: can potentially vote
            None => true,
        };

        if !can_vote {
            debug!(
                "Node {} rejecting vote for {} in term {}: already voted for {:?}",
                self.id,
                candidate_id,
                self.current_term(),
                self.voted_for
            );
            return (false, self.current_term());
        }

        // 4. Check if candidate log is consistent
        let candidate_log_is_consistent =
            self.check_log_consistency(candidate_last_log_index, candidate_last_log_term);

        if !candidate_log_is_consistent {
            debug!(
                "Node {} rejecting vote for {} in term {}: candidate log is not consistent",
                self.id, candidate_id, candidate_term
            );
            return (false, self.current_term());
        }

        // 5. Grant vote
        info!("Node {} voting for candidate {} in term {}", self.id, candidate_id, candidate_term);
        self.voted_for = Some(candidate_id);
        (true, self.current_term())
    }

    /// Record a vote for self during candidate state.
    pub fn record_vote_received(&mut self) {
        if self.state() != NodeState::Candidate {
            warn!("Node {} attempted to record vote for self but is not a candidate", self.id);
            return;
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
