use std::pin::Pin;

use rand::Rng;
use tokio::time::{Duration, Instant, Sleep};

use crate::config::Config;

// TODO: add logging

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
    active_timer: (TimerType, Pin<Box<Sleep>>),
    config: Config,
}

impl NodeTimer {
    pub fn new(config: Config) -> Self {
        // Initialize directly with an election timer
        let initial_type = TimerType::Election;
        let deadline = Self::calculate_deadline(initial_type, &config);
        let initial_timer = (initial_type, Box::pin(tokio::time::sleep_until(deadline)));
        Self { active_timer: initial_timer, config }
    }

    /// Generate a random election timeout within the range of
    /// `ELECTION_TIMEOUT_MIN` and `ELECTION_TIMEOUT_MAX`.
    fn random_election_timeout(config: &Config) -> Duration {
        rand::rng().random_range(config.election_timeout_min..=config.election_timeout_max)
    }

    /// Calculate the deadline for a timer based on its type.
    fn calculate_deadline(timer_type: TimerType, config: &Config) -> Instant {
        let duration = match timer_type {
            TimerType::Election => Self::random_election_timeout(config),
            TimerType::Heartbeat => config.heartbeat_interval,
        };
        Instant::now() + duration
    }

    /// Set the active timer.
    fn set_timer(&mut self, timer_type: TimerType) {
        let deadline = Self::calculate_deadline(timer_type, &self.config);
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
    fn after_max_election(election_timeout_max: Duration) -> Duration {
        election_timeout_max + Duration::from_millis(10)
    }

    // Helper function to get a duration slightly shorter than the min election
    // timeout
    fn before_min_election(election_timeout_min: Duration) -> Duration {
        // Ensure it's not negative if ELECTION_TIMEOUT_MIN is very small
        election_timeout_min.checked_sub(Duration::from_millis(10)).unwrap_or_default()
    }

    // Helper function for heartbeat interval checks
    fn after_heartbeat(heartbeat_interval: Duration) -> Duration {
        heartbeat_interval + Duration::from_millis(10)
    }

    fn before_heartbeat(heartbeat_interval: Duration) -> Duration {
        heartbeat_interval.checked_sub(Duration::from_millis(10)).unwrap_or_default()
    }

    #[tokio::test]
    async fn test_initial_timer_is_election() {
        let config = Config::default();
        // No time manipulation needed here, just check the initial state
        let timer = NodeTimer::new(config);
        assert_eq!(timer.active_timer.0, TimerType::Election, "Initial timer should be Election");
    }

    #[tokio::test]
    async fn test_election_timer_expires_after_timeout() {
        time::pause();

        let config = Config::default();
        let election_timeout_min = config.election_timeout_min;
        let election_timeout_max = config.election_timeout_max;

        let mut timer = NodeTimer::new(config);
        assert_eq!(timer.active_timer.0, TimerType::Election);

        // Check it doesn't expire too early
        let res_early = timeout(
            before_min_election(election_timeout_min),
            timer.wait_for_timer_and_emit_event(),
        )
        .await;
        assert!(res_early.is_err(), "Timer should not expire before minimum election timeout");

        // Advance time past the maximum possible expiry
        time::advance(after_max_election(election_timeout_max)).await;

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

        let config = Config::default();
        let heartbeat_interval = config.heartbeat_interval;
        let mut timer = NodeTimer::new(config);
        // Switch to heartbeat
        timer.reset_heartbeat_timer();
        assert_eq!(timer.active_timer.0, TimerType::Heartbeat);

        // Check it doesn't expire too early
        let res_early =
            timeout(before_heartbeat(heartbeat_interval), timer.wait_for_timer_and_emit_event())
                .await;
        assert!(res_early.is_err(), "Timer should not expire before heartbeat interval");

        // Advance time past the heartbeat interval
        time::advance(after_heartbeat(heartbeat_interval)).await;

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
        let config = Config::default();
        let heartbeat_interval = config.heartbeat_interval;
        let mut timer = NodeTimer::new(config);

        // Switch to heartbeat
        timer.reset_heartbeat_timer();
        assert_eq!(timer.active_timer.0, TimerType::Heartbeat);

        // Advance time past the heartbeat interval
        time::advance(after_heartbeat(heartbeat_interval)).await;

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
        let config = Config::default();
        let election_timeout_max = config.election_timeout_max;
        let mut timer = NodeTimer::new(config);

        // Make sure we start with an election timer
        assert_eq!(timer.active_timer.0, TimerType::Election);

        // Advance time past the election timeout
        time::advance(after_max_election(election_timeout_max)).await;

        // Should expire immediately when awaited
        let res_expired =
            timeout(Duration::from_millis(1), timer.wait_for_timer_and_emit_event()).await;
        assert!(res_expired.is_ok(), "Timer should have expired");

        // Should reset to election
        assert_eq!(timer.active_timer.0, TimerType::Election);
        assert!(timer.active_timer.1.deadline() > Instant::now());
    }
}
