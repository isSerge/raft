#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MessagingError {
    #[error("Destination node {0} not found")]
    NodeNotFound(u64),
    #[error("Failed to send message")]
    SendError,
    #[error("Failed to receive message")]
    ReceiveError,
    #[error("Failed to broadcast message")]
    BroadcastError,
}
