#[derive(Debug, Clone, Default)]
pub struct StateMachine {
    state: u64,
}

impl StateMachine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply(&mut self, command: u64) -> &mut Self {
        self.state += command;
        self
    }

    pub fn get_state(&self) -> u64 {
        self.state
    }
}
