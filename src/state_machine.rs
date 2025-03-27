#[derive(Debug, Clone)]
pub struct StateMachine {
    pub state: u64,
}

impl StateMachine {
    pub fn new() -> Self {
        Self { state: 0 }
    }

    pub fn apply(&mut self, command: u64) {
        self.state += command;
    }
}
