#[derive(Debug, thiserror::Error)]
pub enum MessagingError {
    #[error("Destination node {0} not found")]
    NodeNotFound(u64),
    #[error("Failed to send message")]
    SendError,
    #[error("Failed to receive message")]
    ReceiveError,
    #[error("Mutex lock error")]
    MutexError,
    #[error("Failed to broadcast message")]
    BroadcastError,
}
