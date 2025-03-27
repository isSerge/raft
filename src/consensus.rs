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
            // If candidate_term is greater than current_term, convert to follower
            if candidate_term > self.current_term {
                self.state = NodeState::Follower;
                self.current_term = candidate_term;
                self.voted_for = Some(candidate_id);
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

    pub fn handle_append_entries(&mut self, term: u64) {
        unimplemented!()
    }
}
