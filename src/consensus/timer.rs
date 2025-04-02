use std::pin::Pin;

use rand::Rng;
use tokio::time::{Duration, Instant, Sleep};

// TODO: add logging

// TODO: should be part of config
const ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(150);
const ELECTION_TIMEOUT_MAX: Duration = Duration::from_millis(300);
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(500);

/// Events that can be emitted when a timer expires.
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum TimerType {
    /// Election timer expired.
    Election,
    /// Heartbeat timer expired.
    Heartbeat,
}

/// Timers for elections and heartbeats.
#[derive(Debug)]
pub struct RaftTimers {
    /// The currently active timer and its type.
    active_timer: (TimerType, Pin<Box<Sleep>>), // No longer Option
}

impl RaftTimers {
    pub fn new() -> Self {
        // Initialize directly with an election timer
        let initial_type = TimerType::Election;
        let deadline = Self::calculate_deadline(initial_type);
        let initial_timer = (initial_type, Box::pin(tokio::time::sleep_until(deadline)));
        Self { active_timer: initial_timer }
    }

    /// Generate a random election timeout within the range of
    /// `ELECTION_TIMEOUT_MIN` and `ELECTION_TIMEOUT_MAX`.
    fn random_election_timeout() -> Duration {
        rand::thread_rng().gen_range(ELECTION_TIMEOUT_MIN..=ELECTION_TIMEOUT_MAX)
    }

    /// Calculate the deadline for a timer based on its type.
    fn calculate_deadline(timer_type: TimerType) -> Instant {
        let duration = match timer_type {
            TimerType::Election => Self::random_election_timeout(),
            TimerType::Heartbeat => HEARTBEAT_INTERVAL,
        };
        Instant::now() + duration
    }

    /// Set the active timer.
    fn set_timer(&mut self, timer_type: TimerType) {
        let deadline = Self::calculate_deadline(timer_type);
        // Directly assign the new timer tuple
        self.active_timer = (timer_type, Box::pin(tokio::time::sleep_until(deadline)));
    }

    /// Reset the election timer, regardless of the current active timer.
    pub fn reset_election_timer(&mut self) {
        self.set_timer(TimerType::Election);
    }

    /// Reset the heartbeat timer, regardless of the current active timer.
    pub fn reset_heartbeat_timer(&mut self) {
        self.set_timer(TimerType::Heartbeat);
    }

    pub async fn wait_for_timer_and_emit_event(&mut self) -> TimerType {
        // Get a mutable reference to the timer tuple
        let (timer_type, future) = &mut self.active_timer;

        // Await the pinned future
        future.await;
        let expired_timer_type = *timer_type;

        // Reset the timer based on which one expired
        match expired_timer_type {
            TimerType::Election => self.reset_election_timer(),
            TimerType::Heartbeat => self.reset_heartbeat_timer(),
        }

        expired_timer_type
    }
}
