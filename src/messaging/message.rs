use crate::consensus::LogEntry;

/// A message in the network
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Request vote from other nodes during election
    VoteRequest { term: u64, candidate_id: u64 },
    /// Response to vote request
    VoteResponse { term: u64, vote_granted: bool, from_id: u64 },
    /// Request to append entries to other nodes
    AppendEntries { term: u64, leader_id: u64, new_entries: Vec<LogEntry>, commit_index: u64 },
    /// Response to append request
    AppendResponse { term: u64, success: bool, from_id: u64 },
}
