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
pub struct NodeTimer {
    /// The currently active timer and its type.
    active_timer: (TimerType, Pin<Box<Sleep>>), // No longer Option
}

impl NodeTimer {
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
        rand::rng().random_range(ELECTION_TIMEOUT_MIN..=ELECTION_TIMEOUT_MAX)
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

#[cfg(test)]
mod tests {
    use tokio::time::{self, Duration, timeout};

    use super::*;

    // Helper function to get a duration slightly longer than the max election
    // timeout
    fn after_max_election() -> Duration {
        ELECTION_TIMEOUT_MAX + Duration::from_millis(10)
    }

    // Helper function to get a duration slightly shorter than the min election
    // timeout
    fn before_min_election() -> Duration {
        // Ensure it's not negative if ELECTION_TIMEOUT_MIN is very small
        ELECTION_TIMEOUT_MIN.checked_sub(Duration::from_millis(10)).unwrap_or_default()
    }

    // Helper function for heartbeat interval checks
    fn after_heartbeat() -> Duration {
        HEARTBEAT_INTERVAL + Duration::from_millis(10)
    }

    fn before_heartbeat() -> Duration {
        HEARTBEAT_INTERVAL.checked_sub(Duration::from_millis(10)).unwrap_or_default()
    }

    #[tokio::test]
    async fn test_initial_timer_is_election() {
        // No time manipulation needed here, just check the initial state
        let timer = NodeTimer::new();
        assert_eq!(timer.active_timer.0, TimerType::Election, "Initial timer should be Election");
    }

    #[tokio::test]
    async fn test_election_timer_expires_after_timeout() {
        time::pause();

        let mut timer = NodeTimer::new();
        assert_eq!(timer.active_timer.0, TimerType::Election);

        // Check it doesn't expire too early
        let res_early = timeout(before_min_election(), timer.wait_for_timer_and_emit_event()).await;
        assert!(res_early.is_err(), "Timer should not expire before minimum election timeout");

        // Advance time past the maximum possible expiry
        time::advance(after_max_election()).await;

        // Should expire immediately when awaited
        let res_expired =
            timeout(Duration::from_millis(1), timer.wait_for_timer_and_emit_event()).await;
        assert!(res_expired.is_ok(), "Timer should have expired");
        assert_eq!(
            res_expired.unwrap(),
            TimerType::Election,
            "Expired timer type should be Election"
        );
    }

    #[tokio::test]
    async fn test_heartbeat_timer_expires_after_timeout() {
        time::pause();

        let mut timer = NodeTimer::new();
        // Switch to heartbeat
        timer.reset_heartbeat_timer();
        assert_eq!(timer.active_timer.0, TimerType::Heartbeat);

        // Check it doesn't expire too early
        let res_early = timeout(before_heartbeat(), timer.wait_for_timer_and_emit_event()).await;
        assert!(res_early.is_err(), "Timer should not expire before heartbeat interval");

        // Advance time past the heartbeat interval
        time::advance(after_heartbeat()).await;

        // Should expire immediately when awaited
        let res_expired =
            timeout(Duration::from_millis(1), timer.wait_for_timer_and_emit_event()).await;
        assert!(res_expired.is_ok(), "Timer should have expired");
        assert_eq!(
            res_expired.unwrap(),
            TimerType::Heartbeat,
            "Expired timer type should be Heartbeat"
        );
    }

    #[tokio::test]
    async fn test_auto_reset_after_heartbeat_timeout() {
        time::pause();
        let mut timer = NodeTimer::new();

        // Switch to heartbeat
        timer.reset_heartbeat_timer();
        assert_eq!(timer.active_timer.0, TimerType::Heartbeat);

        // Advance time past the heartbeat interval
        time::advance(after_heartbeat()).await;

        // Should expire immediately when awaited
        let res_expired =
            timeout(Duration::from_millis(1), timer.wait_for_timer_and_emit_event()).await;
        assert!(res_expired.is_ok(), "Timer should have expired");

        // Should reset to heartbeat
        assert_eq!(timer.active_timer.0, TimerType::Heartbeat);
        assert!(timer.active_timer.1.deadline() > Instant::now());
    }

    #[tokio::test]
    async fn test_auto_reset_after_election_timeout() {
        time::pause();
        let mut timer = NodeTimer::new();

        // Make sure we start with an election timer
        assert_eq!(timer.active_timer.0, TimerType::Election);

        // Advance time past the election timeout
        time::advance(after_max_election()).await;

        // Should expire immediately when awaited
        let res_expired =
            timeout(Duration::from_millis(1), timer.wait_for_timer_and_emit_event()).await;
        assert!(res_expired.is_ok(), "Timer should have expired");

        // Should reset to election
        assert_eq!(timer.active_timer.0, TimerType::Election);
        assert!(timer.active_timer.1.deadline() > Instant::now());
    }
}
