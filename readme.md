# Simple Raft Implementation in Rust

This project is my attempt to implement [Raft consensus algorithm](docs/raft.pdf) in Rust for educational purposes. It focuses on demonstrating the core concepts of leader election, log replication, and basic state machine application within a simulated network environment.

## Project Structure

The project is organized into several key modules:

*   **`src/`**
    *   **`main.rs`**: The entry point for the simulation. It sets up the nodes, the simulated network, runs the node tasks, and orchestrates a basic test scenario (leader election, command submission, state verification).
    *   **`config.rs`**: Defines configuration parameters for the Raft nodes, such as timeouts and intervals.
    *   **`state_machine.rs`**:
        *   Defines the `StateMachine` trait, representing the application logic that Raft replicates.
        *   Provides a simple default implementation (`StateMachineDefault`).
    *   **`consensus/`**: Contains the core Raft implementation.
        *   **`mod.rs`**: Exports the public interface of the consensus module.
        *   **`core/`**: Implements the fundamental Raft state and logic, independent of networking or timers.
            *   `mod.rs`: Defines `NodeCore` (holding `currentTerm`, `votedFor`, `log`, `commitIndex`, `lastApplied`, state-specific fields like `nextIndex`, `matchIndex`) and `NodeState` enum. Implements state transitions, voting logic, log consistency checks, and commit index calculation.
            *   `tests.rs`: Unit tests for `NodeCore`.
        *   **`server/`**: Bridges the core logic with the outside world (messaging, timers).
            *   `mod.rs`: Defines `NodeServer` which owns `NodeCore`, a `StateMachine`, a `NodeMessenger`, and handles RPC processing by calling `NodeCore` methods. Contains the main message handling logic (`process_message`, `handle_append_entries`, `handle_request_vote`, etc.) and timer event handling.
            *   `tests.rs`: Integration-style tests for `NodeServer`.
        *   **`timer.rs`**: Implements the election and heartbeat timers using `tokio::time`. Defines `NodeTimer` and `TimerType`.
        *   **`log_entry.rs`**: Defines the `LogEntry` struct stored in the Raft log.
        *   **`error.rs`**: Defines `ConsensusError` for Raft-specific errors.
        *   **`event.rs`**: Defines `ConsensusEvent` (e.g., `LeaderElected`, `EntryCommitted`) for signaling significant occurrences, used by the simulation.
    *   **`messaging/`**: Simulates the network layer for node communication.
        *   **`mod.rs`**: Exports the public interface of the messaging module.
        *   **`network.rs`**: Defines the `Network` struct, which acts as a central router holding channels to each node.
        *   **`messenger.rs`**: Defines `NodeMessenger`, providing an API for a node to send messages (`send_to`, `broadcast`, `send_self`) via the `Network`.
        *   **`receiver.rs`**: Defines `NodeReceiver`, used by each node's task to receive messages from its dedicated channel.
        *   **`message.rs`**: Defines the `Message` enum, containing variants for Raft RPCs and internal commands.
        *   **`error.rs`**: Defines `MessagingError` for network simulation errors.

**Key Design Principles:**

*   **Separation of Concerns:** The core Raft state (`NodeCore`) is separated from the server logic (`NodeServer`) which handles I/O (timers, messages). The network simulation (`messaging`) is also distinct.
*   **Asynchronous:** Leverages `tokio` for non-blocking timers and message passing between nodes.
*   **State Machine Abstraction:** Uses a `StateMachine` trait to decouple the Raft logic from the specific application being replicated.
*   **Simulation Focus:** The current networking layer is designed for in-memory simulation rather than real network sockets.

## Documentation

Cargo generated documentation is hosted on [github pages](https://isserge.github.io/raft/raft/)

## Current State

The implementation currently includes:

*   **Core Raft Logic (`consensus::core`):**
    *   Node states (Follower, Candidate, Leader).
    *   Term management and voting logic (`VoteRequest`, `VoteResponse`).
    *   Basic log replication (`AppendEntries`, `AppendResponse`).
    *   Leader election triggered by timeouts.
    *   Commit index calculation based on majority match.
    *   Log consistency checks and basic conflict handling (truncation).
*   **RPC Handling (`consensus::server`):**
    *   Handles incoming RPC messages (`VoteRequest`, `VoteResponse`, `AppendEntries`, `AppendResponse`).
    *   Manages state transitions based on RPCs and timer events.
    *   Applies committed entries to a simple state machine.
*   **Timers (`consensus::timer`):**
    *   Election timer with randomized timeout.
    *   Heartbeat timer for leaders.
*   **Messaging Simulation (`messaging`):**
    *   In-memory network simulation using `tokio::mpsc` channels.
    *   Ability to broadcast and send messages between simulated nodes.
*   **State Machine (`state_machine`):**
    *   A basic trait (`StateMachine`) and a default implementation (`StateMachineDefault`) that increments a counter.
*   **Simulation (`main.rs`):**
    *   Initializes a cluster of nodes.
    *   Runs node tasks concurrently.
    *   Allows nodes to elect a leader naturally via timeouts.
    *   Sends one or more commands to the elected leader.
    *   Verifies that commands are eventually applied consistently across all nodes' state machines.
*   **Testing:**
    *   Unit tests for core logic (`NodeCore`), timers (`NodeTimer`), and most of server interactions (`NodeServer`).

## How to Run

1.  **Prerequisites:** Ensure you have Rust and Cargo installed (`rustup`).
2.  **Clone:** Clone this repository.
3.  **Build:** `cargo build`
4.  **Run Simulation:** `cargo run`
    *   You can adjust logging levels using the `RUST_LOG` environment variable (e.g., `RUST_LOG=debug cargo run`).
    *   The simulation in `main.rs` currently sets up a small cluster, waits for an election, sends commands to the leader, and verifies state convergence.

## Future Improvements & Next Steps

This implementation provides a foundation, but few critical features are needed for a complete and robust Raft:

1.  **Persistence layer:** Allow nodes to recover their state after crashing.
2.  **Log Compaction (Snapshotting):** Prevent the log from growing indefinitely.
3.  **Client Interaction Guarantees:** Provide correct semantics for external clients submitting commands (including complete commit-wait mechanism)
4.  **Cluster Membership Changes:** Allow adding/removing nodes from the cluster safely.
5.  **More Robust Error Handling:** Implement retry logic for RPCs where appropriate, handle channel closure and other task failures more gracefully.
6.  **Optimizations:** Implement batching of log entries in `AppendEntries`, optimize log persistence.
7.  **Enhanced Simulation:** Test more complex scenarios including network partitions, message loss and delays
8.  **Comprehensive Testing:** More tests for edge cases

