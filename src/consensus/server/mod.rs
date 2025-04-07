#[cfg(test)]
mod tests;

use std::{collections::HashMap, sync::Arc};

use log::{debug, error, info, warn};
use tokio::sync::broadcast;

use crate::{
    consensus::{
        ConsensusError, ConsensusEvent, LogEntry, NodeCore, NodeState, NodeTimer, TimerType,
    },
    messaging::{Message, MessagingError, NodeMessenger},
    state_machine::StateMachine,
};

#[derive(Debug)]
pub struct NodeServer {
    /// The core state of the node.
    core: NodeCore,
    // TODO: consider making state_machine private
    /// The state machine of the node.
    pub state_machine: StateMachine,
    /// The messenger for the node.
    messenger: NodeMessenger,
    /// The event sender for the node.
    event_tx: broadcast::Sender<ConsensusEvent>,
    /// The pending append entries for the node. Used to track the pending
    /// append entries for each follower. Key is follower ID, value is a tuple
    /// of (prev_log_index, entries_len).
    pending_append_entries: HashMap<u64, (u64, usize)>,
}

// NodeServer constructor
impl NodeServer {
    pub fn new(
        id: u64,
        state_machine: StateMachine,
        messenger: NodeMessenger,
        event_tx: broadcast::Sender<ConsensusEvent>,
    ) -> Self {
        Self {
            core: NodeCore::new(id),
            state_machine,
            messenger,
            event_tx,
            pending_append_entries: HashMap::new(),
        }
    }

    // TODO: consider adding constructors for testing with initial state
}

// NodeServer getters (thin wrappers around core methods)
impl NodeServer {
    /// Get the node's ID.
    pub fn id(&self) -> u64 {
        self.core.id()
    }

    /// Get the node's current state.
    pub fn state(&self) -> NodeState {
        self.core.state()
    }

    /// Get the current term.
    pub fn current_term(&self) -> u64 {
        self.core.current_term()
    }

    /// Get the log.
    pub fn log(&self) -> &[LogEntry] {
        self.core.log()
    }

    /// Get the commit index.
    pub fn commit_index(&self) -> u64 {
        self.core.commit_index()
    }

    /// Get the last applied index.
    pub fn last_applied(&self) -> u64 {
        self.core.last_applied()
    }

    /// Get the last log index.
    pub fn log_last_index(&self) -> u64 {
        self.core.log_last_index()
    }

    /// Get the last log term.
    pub fn log_last_term(&self) -> u64 {
        self.core.log_last_term()
    }

    // Getters for testing
    /// Get the match index for a peer.
    #[cfg(test)]
    pub fn match_index_for(&self, peer_id: u64) -> Option<u64> {
        self.core.match_index_for(peer_id)
    }

    /// Get the next index for a peer.
    #[cfg(test)]
    pub fn next_index_for(&self, peer_id: u64) -> Option<u64> {
        self.core.next_index_for(peer_id)
    }

    /// Get the node that this node voted for.
    #[cfg(test)]
    pub fn voted_for(&self) -> Option<u64> {
        self.core.voted_for()
    }
}

// RPC methods
impl NodeServer {
    /// Broadcast a message to all other nodes.
    async fn broadcast(&self, message: Message) -> Result<(), ConsensusError> {
        self.messenger.broadcast(message).await.map_err(ConsensusError::Transport)
    }

    /// Send an AppendResponse to a leader.
    async fn send_append_response(
        &self,
        leader_id: u64,
        success: bool,
        term: u64,
    ) -> Result<(), ConsensusError> {
        let msg = Message::AppendResponse { term, success, from_id: self.id() };
        info!("Node {} sending AppendResponse to leader {}: {:?}", self.id(), leader_id, msg);
        self.messenger.send_to(leader_id, Arc::new(msg)).await.map_err(ConsensusError::Transport)
    }

    /// Send a VoteResponse to a candidate.
    async fn send_vote_response(
        &self,
        candidate_id: u64,
        vote_granted: bool,
        term: u64,
    ) -> Result<(), ConsensusError> {
        let msg = Message::VoteResponse { term, vote_granted, from_id: self.id() };
        info!("Node {} sending VoteResponse to candidate {}: {:?}", self.id(), candidate_id, msg);
        self.messenger.send_to(candidate_id, Arc::new(msg)).await.map_err(ConsensusError::Transport)
    }

    /// Broadcast a vote request to all other nodes.
    async fn broadcast_vote_request(&self) -> Result<(), ConsensusError> {
        if self.core.state() != NodeState::Candidate {
            warn!("Node {} tried to broadcast vote request but is not Candidate", self.id());
            return Err(ConsensusError::NotCandidate(self.id()));
        }

        let term = self.current_term();
        let last_log_index = self.log_last_index();
        let last_log_term = self.log_last_term();
        let msg =
            Message::VoteRequest { term, candidate_id: self.id(), last_log_index, last_log_term };
        info!("Node {} broadcasting VoteRequest: {:?}", self.id(), msg);
        self.broadcast(msg).await
    }

    /// Send an AppendEntries to all followers. Returns Ok(()) if successful,
    /// Err if not leader.
    async fn send_append_entries_to_all_followers(&mut self) -> Result<(), ConsensusError> {
        if self.core.state() != NodeState::Leader {
            warn!("Node {} tried to send AppendEntries to followers but is not Leader", self.id());
            return Err(ConsensusError::NotLeader(self.id()));
        }

        info!("Node {} sending AppendEntries to all followers", self.id());

        let peer_ids = self.messenger.get_peer_ids().await?;

        // Send AppendEntries to all followers
        // Vec to store results from all followers
        let mut results = Vec::new();

        for peer_id in peer_ids {
            // Skip self
            if peer_id == self.id() {
                continue;
            }

            let result = self.send_append_entries_to_follower(peer_id).await;
            results.push(result);
        }

        // TODO: consider more sophisticated error handling
        // Check if any followers failed to receive AppendEntries
        if results.iter().any(|r| r.is_err()) {
            error!("Node {} failed to send AppendEntries to at least one follower", self.id());
            Err(ConsensusError::Transport(MessagingError::BroadcastError))
        } else {
            info!("Node {} successfully sent AppendEntries to all followers", self.id());
            Ok(())
        }
    }

    /// Send an AppendEntries to a follower (used by Leader)
    async fn send_append_entries_to_follower(
        &mut self,
        peer_id: u64,
    ) -> Result<(), ConsensusError> {
        if self.core.state() != NodeState::Leader {
            warn!(
                "Node {} tried to send AppendEntries to follower {} but is not Leader",
                self.id(),
                peer_id
            );
            return Err(ConsensusError::NotLeader(self.id()));
        }

        let current_term = self.current_term();
        let leader_commit = self.commit_index();
        let leader_id = self.id();

        // Get next index and previous log index and term
        let next_index =
            self.core.next_index_for(peer_id).ok_or(ConsensusError::NodeNotFound(self.id()))?;
        let prev_log_index = next_index - 1;
        let prev_log_term = if prev_log_index > 0 {
            self.log().get(prev_log_index as usize).map_or(0, |entry| entry.term)
        } else {
            0
        };

        // Get entries to send
        let entries = if self.log_last_index() >= next_index {
            self.log()[(next_index - 1) as usize..].to_vec()
        } else {
            vec![] // No entries to send - sending heartbeat
        };

        let entries_len = entries.len();

        // store the entries length in the pending append entries map
        self.pending_append_entries.insert(peer_id, (prev_log_index, entries_len));

        // Send message
        let msg = Message::AppendEntries {
            term: current_term,
            leader_id,
            entries,
            prev_log_index,
            prev_log_term,
            leader_commit,
        };

        self.messenger.send_to(peer_id, Arc::new(msg)).await.map_err(ConsensusError::Transport)
    }
}

// Command handlers
impl NodeServer {
    /// Start an election. Used when received Election timer event or start
    /// election command
    async fn start_election(&mut self) -> Result<(), ConsensusError> {
        let new_term = self.current_term() + 1;
        info!("Node {} starting election for term {}", self.id(), new_term);
        self.core.transition_to_candidate();
        self.broadcast_vote_request().await
    }

    /// Start an AppendEntries
    /// Leader appends a new entry to its log. Replication will occur via
    /// heartbeat. Returns Ok(()) if successful, Err if not leader.
    async fn start_append_entries(&mut self, command: String) -> Result<(), ConsensusError> {
        // Delegate appending to the core.
        if self.core.leader_append_entry(command.clone()) {
            info!(
                "Node {} appended new entry to log: {:?}. Replication will occur via heartbeat.",
                self.id(),
                command
            );
            Ok(())
        } else {
            Err(ConsensusError::NotLeader(self.id()))
        }
    }

    /// Handle a request vote from a candidate
    async fn handle_request_vote(
        &mut self,
        candidate_term: u64,
        candidate_id: u64,
        candidate_last_log_index: u64,
        candidate_last_log_term: u64,
        timer: &mut NodeTimer,
    ) -> Result<(), ConsensusError> {
        info!(
            "Node {} received VoteRequest from Node {} for Term {}",
            self.id(),
            candidate_id,
            candidate_term
        );

        // Check if we have to step down
        if candidate_term >= self.current_term() {
            info!(
                "Node {} received VoteRequest for term {} from Node {}, transitioning to Follower.",
                self.id(),
                candidate_term,
                candidate_id
            );
            self.core.transition_to_follower(candidate_term);
            timer.reset_election_timer();
        }

        let (vote_granted, term_to_respond) = self.core.decide_vote(
            candidate_id,
            candidate_term,
            candidate_last_log_index,
            candidate_last_log_term,
        );

        if vote_granted {
            info!(
                "Node {} decided to GRANT vote for Node {} in term {}",
                self.id(),
                candidate_id,
                term_to_respond
            );

            timer.reset_election_timer();
        } else {
            info!(
                "Node {} decided to REJECT vote for Node {} in term {}",
                self.id(),
                candidate_id,
                term_to_respond
            );
        }

        // Send response to candidate
        self.send_vote_response(candidate_id, vote_granted, term_to_respond).await
    }

    /// Handle an AppendEntries request from a leader
    async fn handle_append_entries(
        &mut self,
        leader_term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        new_entries: &[LogEntry],
        leader_commit_index: u64,
        timer: &mut NodeTimer,
    ) -> Result<(), ConsensusError> {
        debug!(
            "Node {} received AppendEntries from Leader {} for Term {}",
            self.id(),
            leader_id,
            leader_term
        );

        // 1. If leader_term is older than current_term, reject
        if leader_term < self.current_term() {
            warn!(
                "Node {} rejecting AppendEntries from Node {} (LeaderTerm {} < CurrentTerm {})",
                self.id(),
                leader_id,
                leader_term,
                self.current_term()
            );
            return self.send_append_response(leader_id, false, self.current_term()).await;
        }

        // Leader is up to date
        // 2. Reset timer
        timer.reset_election_timer();

        // 3. Transition to follower
        self.core.transition_to_follower(leader_term);

        // 4. Check log consistency and append log entries to own log
        let (is_log_consistent, _) =
            self.core.follower_append_entries(prev_log_index, prev_log_term, new_entries);

        if !is_log_consistent {
            warn!(
                "Node {} log is not consistent with Leader {} log. Rejecting AppendEntries.",
                self.id(),
                leader_id
            );
            return self.send_append_response(leader_id, false, self.current_term()).await;
        }

        // 5. Update commit_index
        self.core.follower_update_commit_index(leader_commit_index);

        // 6. Apply log entries to state machine
        self.apply_committed_entries();

        // 7. Reset heartbeat timer
        timer.reset_heartbeat_timer();

        // 8. Send response to leader
        self.send_append_response(leader_id, true, self.current_term()).await
    }

    /// Handle a vote response from a voter. Used by candidates to collect
    /// votes.
    async fn handle_vote_response(
        &mut self,
        term: u64,
        voter_id: u64,
        vote_granted: bool,
        timer: &mut NodeTimer,
    ) -> Result<(), ConsensusError> {
        info!(
            "Node {} received VoteResponse from Node {} for Term {} (Granted: {})",
            self.id(),
            voter_id,
            term,
            vote_granted
        );

        // 1. Perform checks
        if self.core.state() != NodeState::Candidate {
            debug!(
                "Node {} received VoteResponse but is no longer a Candidate. Ignoring.",
                self.id()
            );
            return Ok(());
        }

        if term < self.current_term() {
            debug!(
                "Node {} received VoteResponse for older term {} from Node {}, ignoring.",
                self.id(),
                term,
                voter_id
            );
            return Ok(());
        }

        if term > self.current_term() {
            info!(
                "Node {} sees newer term {} in VoteResponse from Node {}, transitioning to \
                 Follower.",
                self.id(),
                term,
                voter_id
            );
            // Convert to follower and reset election timer
            self.core.transition_to_follower(term);
            timer.reset_election_timer();
            return Ok(());
        }

        // 2. Record vote
        if vote_granted {
            // Get total number of nodes
            let total_nodes = self.messenger.get_nodes_count().await? as u64;

            // Record vote
            self.core.record_vote_received();
            info!(
                "Node {} received vote from Node {}, total votes: {}",
                self.id(),
                voter_id,
                self.core.votes_received()
            );

            // Check if majority of votes have been received
            let majority_count = total_nodes / 2 + 1;

            // If majority of votes have been received, transition to leader
            if self.core.votes_received() >= majority_count {
                info!(
                    "Node {} received majority of votes ({}/{}), becoming Leader for Term {}",
                    self.id(),
                    self.core.votes_received(),
                    total_nodes,
                    self.current_term()
                );

                let peer_ids = self.messenger.get_peer_ids().await?;
                self.core.transition_to_leader(&peer_ids);

                // Start heartbeat timer
                timer.reset_heartbeat_timer();

                // Signal that the node is now a leader
                let _ = self.event_tx.send(ConsensusEvent::LeaderElected { leader_id: self.id() });

                // Send AppendEntries to all other nodes to establish leadership.
                self.send_append_entries_to_all_followers().await?;
            }
        } else {
            info!(
                "Node {} received vote rejection from Node {}, total votes: {}",
                self.id(),
                voter_id,
                self.core.votes_received()
            );
        }

        Ok(())
    }

    /// Handle an AppendResponse from a follower. Used by leaders to
    /// determine if they have received a majority of responses.
    async fn handle_append_response(
        &mut self,
        follower_term: u64,
        success: bool,
        from_id: u64,
        timer: &mut NodeTimer,
    ) -> Result<(), ConsensusError> {
        info!(
            "Node {} (Leader) received AppendResponse from follower {} for term {} (Success: {})",
            self.id(),
            from_id,
            follower_term,
            success
        );

        // Ignore if not leader
        if self.core.state() != NodeState::Leader {
            warn!("Node {} received AppendResponse but is no longer Leader. Ignoring.", self.id());
            // Clear any potentially stale pending info for this follower
            self.pending_append_entries.remove(&from_id);
            return Ok(());
        }

        // If response contains higher term, convert to follower
        if follower_term > self.current_term() {
            info!(
                "Node {} (Leader) sees newer term {} in AppendResponse from follower {}, \
                 transitioning to Follower.",
                self.id(),
                follower_term,
                from_id
            );
            // Clear pending info as we are no longer leader
            self.pending_append_entries.clear();
            self.core.transition_to_follower(follower_term);
            timer.reset_election_timer();
            return Ok(());
        }

        // Ignore if response is for an older term (stale response)
        if follower_term < self.current_term() {
            debug!(
                "Node {} (Leader) received stale AppendResponse from Node {} for term {}. \
                 Ignoring.",
                self.id(),
                from_id,
                follower_term
            );
            // Do not clear pending info here, the follower might just be slow
            return Ok(());
        }

        // Retrieve pending info
        let pending_info = self.pending_append_entries.get(&from_id).cloned();

        // Get total nodes for commit calculation
        let total_nodes = match self.messenger.get_nodes_count().await {
            Ok(count) => count,
            Err(e) => {
                error!(
                    "Node {} (Leader) failed to get node count: {:?}. Cannot process response.",
                    self.id(),
                    e
                );
                // Clear pending info if we can't process
                if pending_info.is_some() {
                    self.pending_append_entries.remove(&from_id);
                }
                return Err(ConsensusError::Transport(e)); // Propagate error
            }
        };

        // Process response for the current term
        let (commit_advanced, _old_ci, _new_ci) =
            if let Some((sent_prev_log_index, sent_entries_len)) = pending_info {
                self.core.leader_process_append_response(
                    from_id,
                    success,
                    sent_prev_log_index,
                    sent_entries_len,
                    total_nodes,
                )
            } else {
                warn!(
                    "Node {} (Leader) received AppendResponse from {} but no pending info found. \
                     Ignoring update.",
                    self.id(),
                    from_id
                );
                (false, self.commit_index(), self.commit_index()) // No change
            };

        // Apply committed entries if commit index has advanced
        if commit_advanced {
            self.apply_committed_entries();
        }

        // Clear pending info now that response is processed
        if pending_info.is_some() {
            self.pending_append_entries.remove(&from_id);
        }

        Ok(())
    }

    /// Send a heartbeat to all other nodes. Used when received Heartbeat timer
    /// event.
    async fn send_heartbeat(&mut self) -> Result<(), ConsensusError> {
        info!("Node {} sending heartbeat to all other nodes", self.id());
        self.send_append_entries_to_all_followers().await
    }

    /// Handle a timer event (election or heartbeat timeout).
    pub async fn handle_timer_event(
        &mut self,
        timer_type: TimerType,
        timer: &mut NodeTimer,
    ) -> Result<(), ConsensusError> {
        match timer_type {
            TimerType::Election =>
                if self.state() != NodeState::Leader {
                    self.start_election().await?;
                } else {
                    warn!(
                        "Node {} received election timer event but is already a Leader. Ignoring.",
                        self.id()
                    );
                    timer.reset_heartbeat_timer();
                },
            TimerType::Heartbeat =>
                if self.state() == NodeState::Leader {
                    self.send_heartbeat().await?;
                } else {
                    warn!(
                        "Node {} received heartbeat timer event but is not a Leader. Ignoring.",
                        self.id()
                    );

                    timer.reset_election_timer();
                },
        }
        Ok(())
    }

    /// Receives and processes a single message or timer event. Used by the
    /// event loop. Returns Ok(()) if processed successfully, Err on error.
    pub async fn process_message(
        &mut self,
        msg: Arc<Message>,
        timer: &mut NodeTimer,
    ) -> Result<(), ConsensusError> {
        match *msg {
            Message::VoteRequest { term, candidate_id, last_log_index, last_log_term } => {
                self.handle_request_vote(term, candidate_id, last_log_index, last_log_term, timer)
                    .await?;
            }
            Message::VoteResponse { term, vote_granted, from_id } => {
                self.handle_vote_response(term, from_id, vote_granted, timer).await?;
            }
            Message::AppendEntries {
                term,
                leader_id,
                ref entries,
                prev_log_index,
                prev_log_term,
                leader_commit,
            } => {
                self.handle_append_entries(
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                    timer,
                )
                .await?;
            }
            Message::AppendResponse { term, success, from_id } => {
                self.handle_append_response(term, success, from_id, timer).await?;
            }
            Message::StartElectionCmd => {
                info!("Node {} received StartElectionCmd", self.id());
                if self.state() != NodeState::Leader {
                    self.start_election().await?;
                } else {
                    warn!(
                        "Node {} received StartElectionCmd but is already a Leader. Ignoring.",
                        self.id()
                    );
                }
            }
            Message::StartAppendEntriesCmd { ref command } => {
                info!("Node {} received StartAppendEntriesCmd: '{}'", self.id(), command);
                if self.state() == NodeState::Leader {
                    let _ = self.start_append_entries(command.clone()).await;
                } else {
                    warn!(
                        "Node {} received StartAppendEntriesCmd but is not a Leader. Ignoring.",
                        self.id()
                    );
                }
            }
        }

        Ok(())
    }
}

// State machine update
impl NodeServer {
    /// Apply committed log entries to the state machine.
    fn apply_committed_entries(&mut self) {
        let commit_idx = self.commit_index(); // Raft index (1-based)
        let mut last_applied = self.last_applied(); // also 1-based; 0 if none applied
        let mut applied_any = false;

        if commit_idx > last_applied {
            info!(
                "Node {} applying entries from index {} up to {}",
                self.id(),
                last_applied + 1,
                commit_idx
            );

            // Use an inclusive range to include commit_idx.
            // Convert Raft index to Vec index by subtracting 1.
            for i in (last_applied + 1)..=commit_idx {
                if let Some(entry) = self.log().get((i - 1) as usize) {
                    info!(
                        "Node {} applying log[{}] ('{}') to state machine.",
                        self.id(),
                        i,
                        entry.command
                    );
                    // Apply the command to the state machine
                    self.state_machine.apply(1);
                    last_applied = i; // Update last_applied after successful apply
                    applied_any = true;
                    info!(
                        "   -> Node {} new state machine value: {}",
                        self.id(),
                        self.state_machine.get_state()
                    );
                } else {
                    error!(
                        "Node {} CRITICAL: Tried to apply non-existent log entry at Raft index {}",
                        self.id(),
                        i
                    );
                    break; // Stop applying if log entry missing
                }
            }

            // Update core's last_applied state after the loop finishes.
            self.core.set_last_applied(last_applied);
        }

        if applied_any {
            let _ = self.event_tx.send(ConsensusEvent::EntryCommitted {
                index: commit_idx,
                entry: self
                    .log()
                    // Use (commit_idx - 1) to convert to Vec index.
                    .get((commit_idx - 1) as usize)
                    .expect("Log should not be empty after applying entries")
                    .command
                    .clone(),
            });
        }
    }
}
