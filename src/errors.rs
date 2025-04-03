#[derive(Debug)]
pub enum WsDataError {
    // ... existing errors ...
    WaitTaskError {
        reason: String,
    },
}

impl std::fmt::Display for WsDataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // ... existing error matches ...
            WsDataError::WaitTaskError { reason } => {
                write!(f, "Failed to wait for tasks: {}", reason)
            }
        }
    }
} 