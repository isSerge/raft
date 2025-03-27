use crate::{messaging::NodeMessenger, state_machine::StateMachine};

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

    pub fn handle_request_vote(&mut self, term: u64) -> bool {
        unimplemented!()
    }

    pub fn handle_append_entries(&mut self, term: u64) -> bool {
        unimplemented!()
    }
}
