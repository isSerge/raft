#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsensusEvent {
    /// A new leader has been elected.
    LeaderElected { leader_id: u64 },
    /// A new entry has been committed.
    EntryCommitted { index: u64, entry: String },
}
