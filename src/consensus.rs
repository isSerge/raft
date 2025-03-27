use crate::{
    messaging::{Message, NodeMessenger},
    state_machine::StateMachine,
};

#[derive(Debug, Clone, PartialEq)]
pub enum NodeState {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub term: u64,
    pub command: String,
}

#[derive(Debug)]
pub struct Node {
    pub id: u64,
    pub state: NodeState,
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub state_machine: StateMachine,
    pub messenger: NodeMessenger,
    pub log: Vec<LogEntry>,
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
            log: vec![],
        }
    }

    /// Handle a request vote from a candidate
    pub fn handle_request_vote(&mut self, candidate_term: u64, candidate_id: u64) {
        let response_message: Message;
        // 1. If candidate_term is older than current_term, reject
        if candidate_term < self.current_term {
            response_message =
                Message::VoteResponse { term: self.current_term, vote_granted: false };
        } else {
            // 2. If candidate_term is greater than current_term, convert to follower and
            //    reset
            // voted_for
            if candidate_term > self.current_term {
                self.state = NodeState::Follower;
                self.current_term = candidate_term;
                self.voted_for = None;
            }

            // 3. Vote if we haven't voted yet.
            if self.voted_for.is_none() {
                self.voted_for = Some(candidate_id);
                response_message =
                    Message::VoteResponse { term: self.current_term, vote_granted: true };
            } else {
                response_message =
                    Message::VoteResponse { term: self.current_term, vote_granted: false };
            }
        }

        self.messenger.send_to(self.id, candidate_id, response_message);
    }

    /// Handle an AppendEntries request from a leader
    pub fn handle_append_entries(
        &mut self,
        leader_term: u64,
        leader_id: u64,
        new_entries: Vec<LogEntry>,
    ) {
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

            // 3. append log entries to own log
            self.log.extend(new_entries.clone());

            // 4. update state_machine
            self.state_machine.apply(1 + new_entries.len() as u64);

            response_message = Message::AppendResponse { term: self.current_term, success: true };
        }

        // send response to leader
        self.messenger.send_to(self.id, leader_id, response_message);
    }

    pub fn append_to_log(&mut self, command: String) {
        // Ensure that only a leader can append a new command.
        if self.state != NodeState::Leader {
            println!("Node {} is not a leader. Cannot append new command.", self.id);
            return;
        }

        // Append the new entry to the log.
        let new_entry = LogEntry { term: self.current_term, command };
        self.log.push(new_entry.clone());
        println!("Leader Node {} appended new log entry: {:?}", self.id, new_entry);

        // Update the state machine.
        self.state_machine.apply(1);
        println!("Leader Node {} updated its own state machine.", self.id);

        // Broadcast the new log entry to all other nodes.
        self.messenger.broadcast(
            self.id,
            Message::AppendEntries {
                term: self.current_term,
                leader_id: self.id,
                new_entries: vec![new_entry],
            },
        );
    }
}
