#[derive(Debug, Clone, PartialEq, Eq)]

/// A log entry in the Raft log.
pub struct LogEntry {
    pub term: u64,
    pub command: String,
}

impl LogEntry {
    pub fn new(term: u64, command: String) -> Self {
        Self { term, command }
    }
}
