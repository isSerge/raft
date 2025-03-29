#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MessagingError {
    #[error("Destination node {0} not found")]
    NodeNotFound(u64),
    #[error("Failed to send message to node {0}")]
    SendError(u64),
    #[error("Failed to receive message from node {0}")]
    ReceiveError(u64),
    #[error("Failed to broadcast message")]
    BroadcastError,
}
