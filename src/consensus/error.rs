use crate::messaging::MessagingError;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConsensusError {
    #[error("Timeout: {0}")]
    Timeout(String),
    #[error("Node {0} not found")]
    NodeNotFound(u64),
    #[error("Node {0} is not a leader")]
    NotLeader(u64),
    #[error("Node {0} is not a candidate")]
    NotCandidate(u64),
    #[error("Message handling failed: {0}")]
    Transport(#[from] MessagingError),
}
