use crate::consensus::LogEntry;

/// A message in the network or a command to a node
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    // Raft messages
    /// Request vote from other nodes during election
    VoteRequest { term: u64, candidate_id: u64 },
    /// Response to vote request
    VoteResponse { term: u64, vote_granted: bool, from_id: u64 },
    /// Request to append entries to other nodes
    AppendEntries { term: u64, leader_id: u64, new_entries: Vec<LogEntry>, commit_index: u64 },
    /// Response to append request
    AppendResponse { term: u64, success: bool, from_id: u64 },

    // Commands
    /// Command to start an election
    StartElectionCmd,
    /// Command to append a new entry to the log
    StartAppendEntriesCmd { command: String },
}
