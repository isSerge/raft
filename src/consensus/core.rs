use std::collections::HashMap;

use log::{debug, info, warn};

use crate::consensus::LogEntry;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NodeState {
    #[default]
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Default)]
pub struct NodeCore {
    id: u64,
    state: NodeState,
    current_term: u64,
    voted_for: Option<u64>,
    log: Vec<LogEntry>,
    commit_index: u64,
    votes_received: u64,
    last_applied: u64,
    next_index: HashMap<u64, u64>,
    match_index: HashMap<u64, u64>,
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

    pub fn log_last_index(&self) -> u64 {
        self.log.len() as u64
    }
}

// Setters
impl NodeCore {
    pub fn set_last_applied(&mut self, last_applied: u64) {
        self.last_applied = last_applied;
    }

    pub fn set_voted_for(&mut self, voted_for: Option<u64>) {
        self.voted_for = voted_for;
    }

    pub fn increment_votes_received(&mut self) {
        self.votes_received += 1;
    }
}

impl NodeCore {
    pub fn new(id: u64) -> Self {
        Self { id, ..Default::default() }
    }

    /// Transition to a follower state.
    pub fn transition_to_follower(&mut self, term: u64) {
        if term > self.current_term() {
            self.current_term = term;
            self.voted_for = None;
        }
        self.state = NodeState::Follower;
        self.votes_received = 0;
    }

    /// Transition to a candidate state.
    pub fn transition_to_candidate(&mut self, term: u64) {
        self.current_term = term.max(self.current_term());
        self.state = NodeState::Candidate;
        self.voted_for = Some(self.id);
        self.votes_received = 1; // add self vote
    }

    /// Transition to a leader state.
    pub fn transition_to_leader(&mut self, term: u64) {
        self.current_term = term.max(self.current_term());
        self.state = NodeState::Leader;
        self.voted_for = Some(self.id);
    }

    /// Append log entries to the node's log.
    pub fn append_log_entries(&mut self, new_entries: Vec<LogEntry>) {
        self.log.extend_from_slice(&new_entries);
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
}
