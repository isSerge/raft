use crate::messaging::MessagingError;

#[derive(Debug, thiserror::Error)]
pub enum ConsensusError {
    #[error("Node {0} not found")]
    NodeNotFound(u64),
    #[error("Node {0} is not a leader")]
    NotLeader(u64),
    #[error("Node {0} is not a candidate")]
    NotCandidate(u64),
    #[error("Node {0} is not a follower")]
    NotFollower(u64),
    #[error("Message handling failed: {0}")]
    Transport(#[from] MessagingError),
    // TODO: add log inconsistency error
}
