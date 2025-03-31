use std::sync::Arc;

use log::{debug, error, info, warn};

use crate::{
    consensus::{ConsensusError, LogEntry, NodeCore, NodeState},
    messaging::{Message, NodeMessenger, NodeReceiver},
    state_machine::StateMachine,
};

#[derive(Debug)]
pub struct NodeServer {
    // TODO: make private after tests are updated
    pub core: NodeCore,
    pub state_machine: StateMachine,
    messenger: NodeMessenger,
    receiver: NodeReceiver,
}

// NodeServer constructor
impl NodeServer {
    pub fn new(
        id: u64,
        state_machine: StateMachine,
        messenger: NodeMessenger,
        receiver: NodeReceiver,
    ) -> Self {
        Self { core: NodeCore::new(id), state_machine, messenger, receiver }
    }
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

    /// Get the node that this node voted for.
    pub fn voted_for(&self) -> Option<u64> {
        self.core.voted_for()
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
}

// RPC methods
impl NodeServer {
    /// Receive a message from the network.
    pub async fn receive_message(&mut self) -> Result<Arc<Message>, ConsensusError> {
        self.receiver.receive().await.map_err(ConsensusError::Transport)
    }

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
    pub async fn broadcast_vote_request(&self) -> Result<(), ConsensusError> {
        if self.core.state() != NodeState::Candidate {
            warn!("Node {} tried to broadcast vote request but is not Candidate", self.id());
            return Err(ConsensusError::NotCandidate(self.id()));
        }

        let term = self.core.current_term();
        let msg = Message::VoteRequest { term, candidate_id: self.id() };
        info!("Node {} broadcasting VoteRequest: {:?}", self.id(), msg);
        self.broadcast(msg).await
    }

    /// Broadcast an AppendEntries request to all other nodes.
    pub async fn broadcast_append_entries(
        &self,
        new_entries: Vec<LogEntry>,
    ) -> Result<(), ConsensusError> {
        if self.core.state() != NodeState::Leader {
            warn!("Node {} tried to broadcast AppendEntries but is not Leader", self.id());
            return Err(ConsensusError::NotLeader(self.id()));
        }

        let term = self.core.current_term();
        let leader_id = self.id();
        let commit_index = self.core.commit_index();

        info!(
            "Node {} (Leader Term: {}) broadcasting AppendEntries: commit_index={}, entries={}",
            self.id(),
            self.core.current_term(),
            self.core.commit_index(),
            new_entries.len()
        );

        let msg = Message::AppendEntries { term, leader_id, new_entries, commit_index };

        self.broadcast(msg).await
    }
}

// Command handlers
impl NodeServer {
    /// Start an election.
    pub async fn start_election(&mut self) -> Result<(), ConsensusError> {
        let new_term = self.core.current_term() + 1;
        info!("Node {} starting election for term {}", self.id(), new_term);
        self.core.transition_to_candidate();
        self.broadcast_vote_request().await
    }

    /// Start an AppendEntries
    pub async fn start_append_entries(&mut self, command: String) -> Result<(), ConsensusError> {
        // Delegate appending to the core.
        if self.core.leader_append_entry(command) {
            let new_entry =
                self.core.log().last().expect("Log should not be empty after appending").clone();
            info!(
                "Node {} appended new entry to log: {:?}. Broadcasting to all other nodes.",
                self.id(),
                new_entry
            );
            // Broadcast the new log entry to all other nodes.
            self.broadcast_append_entries(vec![new_entry]).await
        } else {
            Err(ConsensusError::NotLeader(self.id()))
        }
    }

    /// Handle a request vote from a candidate
    pub async fn handle_request_vote(
        &mut self,
        candidate_term: u64,
        candidate_id: u64,
    ) -> Result<(), ConsensusError> {
        info!(
            "Node {} received VoteRequest from Node {} for Term {}",
            self.id(),
            candidate_id,
            candidate_term
        );

        let (vote_granted, term_to_respond) = self.core.decide_vote(candidate_id, candidate_term);
        println!(
            "Node {} decided to {} vote for Node {} in term {}",
            self.id(),
            vote_granted,
            candidate_id,
            term_to_respond
        );
        self.send_vote_response(candidate_id, vote_granted, term_to_respond).await
    }

    /// Handle an AppendEntries request from a leader
    pub async fn handle_append_entries(
        &mut self,
        leader_term: u64,
        leader_id: u64,
        new_entries: &[LogEntry],
        leader_commit_index: u64,
    ) -> Result<(), ConsensusError> {
        debug!(
            "Node {} received AppendEntries from Leader {} for Term {}",
            self.id(),
            leader_id,
            leader_term
        );

        // 1. If leader_term is older than current_term, reject
        if leader_term < self.core.current_term() {
            warn!(
                "Node {} rejecting AppendEntries from Node {} (LeaderTerm {} < CurrentTerm {})",
                self.id(),
                leader_id,
                leader_term,
                self.core.current_term()
            );
            return self.send_append_response(leader_id, false, self.core.current_term()).await;
        }

        // 2. If leader_term is equal or greater than current_term transition to
        //    follower
        self.core.transition_to_follower(leader_term);

        // 3. Check log consistency and append log entries to own log
        let (is_log_consistent, _log_modified) = self.core.follower_append_entries(new_entries);

        if !is_log_consistent {
            warn!(
                "Node {} log is not consistent with Leader {} log. Rejecting AppendEntries.",
                self.id(),
                leader_id
            );
            return self.send_append_response(leader_id, false, self.core.current_term()).await;
        }

        // 5. Update commit_index
        self.core.follower_update_commit_index(leader_commit_index);

        // 5. apply log entries to state machine
        self.apply_committed_entries();

        // 6. send response to leader
        self.send_append_response(leader_id, true, self.core.current_term()).await
    }

    /// Handle a vote response from a voter. Used by candidates to collect
    /// votes.
    pub async fn handle_vote_response(
        &mut self,
        term: u64,
        voter_id: u64,
        vote_granted: bool,
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

        if term < self.core.current_term() {
            debug!(
                "Node {} received VoteResponse for older term {} from Node {}, ignoring.",
                self.id(),
                term,
                voter_id
            );
            return Ok(());
        }

        if term > self.core.current_term() {
            info!(
                "Node {} sees newer term {} in VoteResponse from Node {}, transitioning to \
                 Follower.",
                self.id(),
                term,
                voter_id
            );
            self.core.transition_to_follower(term);
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

            if self.core.votes_received() >= majority_count {
                info!(
                    "Node {} received majority of votes ({}/{}), becoming Leader for Term {}",
                    self.id(),
                    self.core.votes_received(),
                    total_nodes,
                    self.core.current_term()
                );

                // Transition to leader
                self.core.transition_to_leader();

                // Send an empty AppendEntries to all other nodes to establish leadership.
                self.broadcast_append_entries(vec![]).await?;
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
    pub async fn handle_append_response(
        &mut self,
        term: u64,
        success: bool,
        from_id: u64,
    ) -> Result<(), ConsensusError> {
        info!(
            "Node {} (Leader) received AppendResponse from follower {} for term {} (Success: {})",
            self.id(),
            from_id,
            term,
            success
        );

        // 1. Perform checks
        if self.core.state() != NodeState::Leader {
            debug!(
                "Node {} (Leader) received AppendResponse but is no longer a Leader. Ignoring.",
                self.id()
            );
            return Ok(());
        }

        if term > self.core.current_term() {
            info!(
                "Node {} (Leader) sees newer term {} in AppendResponse from follower {}, \
                 transitioning to Follower.",
                self.id(),
                term,
                from_id
            );
            self.core.transition_to_follower(term);
            return Ok(());
        }

        if term < self.core.current_term() {
            debug!(
                "Node {} (Leader) received stale AppendResponse from Node {} for term {}. \
                 Ignoring.",
                self.id(),
                from_id,
                term
            );
            return Ok(());
        }

        // 2. Delegate commit index update logic to core
        let total_nodes = self.messenger.get_nodes_count().await.unwrap_or(1);
        let commit_result =
            self.core.leader_update_commit_index(from_id, success, total_nodes as u64);

        // If commit index advanced, apply to *leader's* state machine
        if let Some((_old_ci, _new_ci)) = commit_result {
            self.apply_committed_entries();
        }

        // TODO: Trigger resend if success was false and next_index was decremented

        Ok(())
    }

    /// Continuously process incoming messages.
    pub async fn process_incoming_messages(&mut self) -> Result<(), ConsensusError> {
        info!("Node {} starting message processing loop.", self.id());
        loop {
            let msg_arc = self.receive_message().await;

            match msg_arc {
                Ok(msg_arc) => match *msg_arc {
                    Message::VoteRequest { term, candidate_id } => {
                        self.handle_request_vote(term, candidate_id).await?;
                    }
                    Message::VoteResponse { term, vote_granted, from_id } => {
                        self.handle_vote_response(term, from_id, vote_granted).await?;
                    }
                    Message::AppendEntries { term, leader_id, ref new_entries, commit_index } => {
                        self.handle_append_entries(term, leader_id, new_entries, commit_index)
                            .await?;
                    }
                    Message::AppendResponse { term, success, from_id } => {
                        self.handle_append_response(term, success, from_id).await?;
                    }
                    Message::StartElectionCmd => {
                        self.start_election().await?;
                    }
                    Message::StartAppendEntriesCmd { ref command } => {
                        if self.core.state() == NodeState::Leader {
                            self.start_append_entries(command.clone()).await?;
                        } else {
                            warn!(
                                "Node {} received StartAppendEntriesCmd but is not a Leader. \
                                 Ignoring.",
                                self.id()
                            );
                        }
                    }
                },
                Err(e) => {
                    error!("Node {} failed to receive message: {:?}. Stopping loop.", self.id(), e);
                    return Err(e);
                }
            }

            // Yield to other tasks to prevent busy-waiting
            tokio::task::yield_now().await;
        }
    }
}

// State machine update
impl NodeServer {
    /// Apply committed log entries to the state machine.
    fn apply_committed_entries(&mut self) {
        let commit_idx = self.core.commit_index();
        let mut last_applied = self.core.last_applied();

        if commit_idx > last_applied {
            info!(
                "Node {} applying entries from index {} up to {}",
                self.id(),
                last_applied + 1,
                commit_idx
            );

            for i in (last_applied + 1)..commit_idx {
                if let Some(entry) = self.core.log().get(i as usize) {
                    info!(
                        "Node {} applying log[{}] ('{}') to state machine.",
                        self.id(),
                        i,
                        entry.command
                    );
                    // Apply the command to the state machine
                    self.state_machine.apply(1);
                    last_applied = i; // Update last_applied *after* successful apply
                    info!(
                        "   -> Node {} new state machine value: {}",
                        self.id(),
                        self.state_machine.get_state()
                    );
                } else {
                    error!(
                        "Node {} CRITICAL: Tried to apply non-existent log entry at index {}",
                        self.id(),
                        i
                    );
                    break; // Stop applying if log entry missing
                }
            }

            // Update core's last_applied state *after* the loop finishes
            self.core.set_last_applied(last_applied);
        }
    }
}
