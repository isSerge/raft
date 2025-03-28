use crate::{
    consensus::{ConsensusError, LogEntry},
    messaging::{Message, NodeMessenger},
    state_machine::StateMachine,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug, Clone)]
pub struct Node {
    id: u64,
    state: NodeState,
    current_term: u64,
    voted_for: Option<u64>,
    pub state_machine: StateMachine,
    messenger: NodeMessenger,
    log: Vec<LogEntry>,
}

// Node getters
impl Node {
    /// Get the node's ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get the node's current state.
    pub fn state(&self) -> NodeState {
        self.state
    }

    /// Get the current term.
    pub fn current_term(&self) -> u64 {
        self.current_term
    }

    /// Get the node that this node voted for.
    pub fn voted_for(&self) -> Option<u64> {
        self.voted_for
    }

    /// Get the log.
    pub fn log(&self) -> &[LogEntry] {
        &self.log
    }
}

// Node message methods (thin wrappers around messenger methods)
impl Node {
    /// Receive a message from the network.
    pub async fn receive_message(&mut self) -> Result<Message, ConsensusError> {
        self.messenger.receive().await.map_err(ConsensusError::Transport)
    }

    /// Broadcast a message to all other nodes.
    async fn broadcast(&self, message: Message) -> Result<(), ConsensusError> {
        self.messenger.broadcast(self.id, message).await.map_err(ConsensusError::Transport)
    }

    /// Send an AppendResponse to a leader.
    async fn send_append_response(
        &self,
        leader_id: u64,
        success: bool,
    ) -> Result<(), ConsensusError> {
        let msg = Message::AppendResponse { term: self.current_term, success };
        self.messenger.send_to(self.id, leader_id, msg).await.map_err(ConsensusError::Transport)
    }

    /// Send a VoteResponse to a candidate.
    async fn send_vote_response(
        &self,
        candidate_id: u64,
        vote_granted: bool,
    ) -> Result<(), ConsensusError> {
        let msg = Message::VoteResponse { term: self.current_term, vote_granted };
        self.messenger.send_to(self.id, candidate_id, msg).await.map_err(ConsensusError::Transport)
    }

    pub async fn broadcast_vote_request(&self) -> Result<(), ConsensusError> {
        if !matches!(self.state, NodeState::Candidate) {
            return Err(ConsensusError::NotCandidate(self.id));
        }

        let msg = Message::VoteRequest { term: self.current_term, candidate_id: self.id };
        self.broadcast(msg).await
    }

    pub async fn broadcast_append_entries(
        &self,
        new_entries: Vec<LogEntry>,
    ) -> Result<(), ConsensusError> {
        let msg =
            Message::AppendEntries { term: self.current_term, leader_id: self.id, new_entries };
        self.broadcast(msg).await
    }
}

// Node core methods
impl Node {
    pub fn new(id: u64, state_machine: StateMachine, messenger: NodeMessenger) -> Self {
        Self {
            id,
            state: NodeState::Follower,
            current_term: 0,
            voted_for: None,
            state_machine,
            messenger,
            log: vec![],
        }
    }

    /// Transition to a new state.
    pub fn transition_to(&mut self, state: NodeState, term: u64) {
        match state {
            NodeState::Follower => {
                if term > self.current_term {
                    self.current_term = term;
                    self.voted_for = None;
                }
                self.state = NodeState::Follower;
            }
            NodeState::Candidate => {
                self.current_term = term.max(self.current_term);
                self.state = NodeState::Candidate;
                self.voted_for = Some(self.id);
            }
            NodeState::Leader => {
                self.current_term = term.max(self.current_term);
                self.state = NodeState::Leader;
                self.voted_for = Some(self.id);
            }
        }
    }

    /// Handle a request vote from a candidate
    pub async fn handle_request_vote(
        &mut self,
        candidate_term: u64,
        candidate_id: u64,
    ) -> Result<(), ConsensusError> {
        // 1. If candidate_term is older than current_term, reject
        if candidate_term < self.current_term {
            return self.send_vote_response(candidate_id, false).await;
        }
        // 2. If candidate_term is greater than current_term, convert to follower and
        //    reset voted_for
        if candidate_term > self.current_term {
            self.transition_to(NodeState::Follower, candidate_term);
        }

        // 3. Vote if we haven't voted yet.
        let can_vote = self.voted_for.is_none_or(|voted_for| voted_for == candidate_id);
        if can_vote {
            self.voted_for = Some(candidate_id);
        }

        self.send_vote_response(candidate_id, can_vote).await
    }

    /// Handle an AppendEntries request from a leader
    pub async fn handle_append_entries(
        &mut self,
        leader_term: u64,
        leader_id: u64,
        new_entries: &[LogEntry],
    ) -> Result<(), ConsensusError> {
        // If leader_term is older than current_term, reject
        if leader_term < self.current_term {
            return self.send_append_response(leader_id, false).await;
        }
        // If leader_term is equal or greater than current_term:
        // 1. convert to follower
        self.transition_to(NodeState::Follower, leader_term);

        // 2. append log entries to own log
        self.log.extend(new_entries.iter().cloned());

        // 3. update state_machine
        self.state_machine.apply(new_entries.len() as u64);

        // 4. send response to leader
        self.send_append_response(leader_id, true).await
    }

    pub async fn append_to_log_and_broadcast(
        &mut self,
        command: String,
    ) -> Result<(), ConsensusError> {
        // Ensure that only a leader can append a new command.
        if !matches!(self.state, NodeState::Leader) {
            return Err(ConsensusError::NotLeader(self.id));
        }

        // Append the new entry to the log.
        let new_entry = LogEntry::new(self.current_term, command);
        self.log.push(new_entry.clone());
        println!("Leader Node {} appended new log entry: {:?}", self.id, new_entry);

        // Update the state machine.
        self.state_machine.apply(1);
        println!("Leader Node {} updated its own state machine.", self.id);

        // Broadcast the new log entry to all other nodes.
        self.broadcast_append_entries(vec![new_entry]).await
    }
}
