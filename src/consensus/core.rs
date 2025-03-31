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

    /// Get the log entry at the given Raft index.
    /// Raft index is 1-based, but Vec index is 0-based.
    pub fn log_entry_at_index(&self, raft_index: u64) -> Option<&LogEntry> {
        if raft_index > 0 { self.log.get((raft_index - 1) as usize) } else { None }
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
        self.log.last().map_or(0, |entry| entry.term)
    }

    /// Check if the node has received majority of votes.
    pub fn has_majority_votes(&self) -> bool {
        // TODO: get total number of nodes from cluster
        let total_nodes = 2;
        let majority_count = total_nodes / 2 + 1;
        self.votes_received() >= majority_count
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

    /// Record a vote for a candidate if not already voted for someone else.
    pub fn set_voted_for(&mut self, candidate_id: u64) -> bool {
        match self.voted_for() {
            Some(voted_for) => voted_for == candidate_id,
            None => {
                self.voted_for = Some(candidate_id);
                true
            }
        }
    }

    /// Increment the votes received.
    pub fn increment_votes_received(&mut self) {
        self.votes_received += 1;
    }

    /// Update the term of the node and reset the vote if new_term is greater
    /// than current_term. Returns true if the term was updated, false
    /// otherwise.
    pub fn update_term(&mut self, new_term: u64) -> bool {
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

        if term_updated && self.state() != NodeState::Follower {
            info!("Node {} transitioning to follower state at term {}", self.id, term);
            self.state = NodeState::Follower;
            self.votes_received = 0; // reset votes
        }
    }

    /// Transition to a candidate and vote for self.
    pub fn transition_to_candidate(&mut self) {
        if self.state() != NodeState::Follower {
            warn!(
                "Node {} attempted to transition to candidate state but is not a follower",
                self.id
            );
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

    /// Check if the log is consistent with another log.
    // TODO: Implement this.
    pub fn check_log_consistency(&self) -> bool {
        true
    }

    // Voting methods
    /// Decide whether to vote for a candidate.
    fn decide_vote(&mut self, candidate_id: u64, candidate_term: u64) -> bool {
        // 1. If candidate_term is older than current_term, reject
        if candidate_term < self.current_term() {
            return false;
        }
        // 2. If candidate_term is greater than current_term, convert to follower and
        //    reset voted_for
        if candidate_term > self.current_term() {
            self.transition_to_follower(candidate_term);
        }

        // TODO: check candidate log is consistent

        // 3. Vote if we haven't voted yet.
        let voted_granted = self.set_voted_for(candidate_id);
        if voted_granted {
            info!(
                "Node {} voted for candidate {} in term {}",
                self.id, candidate_id, candidate_term
            );
        } else {
            debug!(
                "Node {} did not vote for candidate {} in term {}",
                self.id, candidate_id, candidate_term
            );
        }

        voted_granted
    }

    /// Record a vote for self during candidate state.
    fn record_vote_and_check_majority(&mut self) -> bool {
        if self.state() != NodeState::Candidate {
            warn!("Node {} attempted to record vote for self but is not a candidate", self.id);
            return false;
        }

        self.increment_votes_received();
        info!(
            "Node {} received vote from Node {}, total votes: {}",
            self.id,
            self.id,
            self.votes_received()
        );

        self.has_majority_votes()
    }
}
