use std::fmt::Debug;

/// A trait for a state machine.
pub trait StateMachine: Send + Sync + Debug {
    /// Apply a command to the state machine.
    fn apply(&mut self, command: u64);
    /// Get the state of the state machine.
    fn get_state(&self) -> u64;
}

#[derive(Debug, Clone, Default)]
pub struct StateMachineDefault {
    state: u64,
}

impl StateMachine for StateMachineDefault {
    fn apply(&mut self, command: u64) {
        self.state += command;
    }

    fn get_state(&self) -> u64 {
        self.state
    }
}

impl StateMachineDefault {
    pub fn new() -> Self {
        Self::default()
    }
}
