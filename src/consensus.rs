use crate::{
    messaging::{Message, NodeMessenger},
    state_machine::StateMachine,
};

#[derive(Debug, Clone)]
pub enum NodeState {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug)]
pub struct Node {
    pub id: u64,
    pub state: NodeState,
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub state_machine: StateMachine,
    pub messenger: NodeMessenger,
    // TODO: add log
}

impl Node {
    pub fn new(id: u64, state_machine: StateMachine, messenger: NodeMessenger) -> Self {
        Self {
            id,
            state: NodeState::Follower,
            current_term: 0,
            voted_for: None,
            state_machine,
            messenger,
        }
    }

    /// Handle a request vote from a candidate
    pub fn handle_request_vote(&mut self, candidate_term: u64, candidate_id: u64) {
        let response_message: Message;
        // If candidate_term is older than current_term, reject
        if candidate_term < self.current_term {
            response_message =
                Message::VoteResponse { term: self.current_term, vote_granted: false };
        } else {
            // If candidate_term is greater than current_term, convert to follower and reset
            // voted_for
            if candidate_term > self.current_term {
                self.state = NodeState::Follower;
                self.current_term = candidate_term;
                self.voted_for = None;
            }

            // if haven't voted for anyone, vote for candidate
            if self.voted_for.is_none() {
                self.voted_for = Some(candidate_id);

                response_message =
                    Message::VoteResponse { term: self.current_term, vote_granted: true };
            } else {
                // if already voted for someone, reject
                response_message =
                    Message::VoteResponse { term: self.current_term, vote_granted: false };
            }
        }

        self.messenger.send_to(self.id, candidate_id, response_message);
    }

    pub fn handle_append_entries(&mut self, leader_term: u64, leader_id: u64) {
        let response_message: Message;

        // If leader_term is older than current_term, reject
        if leader_term < self.current_term {
            response_message = Message::AppendResponse { term: self.current_term, success: false };
        } else {
            // If leader_term is equal or greater than current_term:
            // 1. update current_term
            self.current_term = leader_term;

            // 2. convert to follower (in case it was a candidate)
            self.state = NodeState::Follower;

            // 3. update state_machine
            // TODO: apply log entries
            self.state_machine.apply(1);

            response_message = Message::AppendResponse { term: self.current_term, success: true };
        }

        // send response to leader
        self.messenger.send_to(self.id, leader_id, response_message);
    }
}
