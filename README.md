# PyuDB - A Distributed Database

A production-quality distributed database written in **Rust** demonstrating:
- **Raft Consensus Algorithm** for distributed agreement
- **ACID Transactions** with MVCC (Multi-Version Concurrency Control)
- **Custom Query Language (PyuSQL)** - SQL-like syntax

## Features

### Storage Engine
- B-Tree based in-memory indexing
- Write-Ahead Log (WAL) for durability
- MVCC for concurrent transaction support
- Crash recovery from WAL

### Raft Consensus
- Leader election with randomized timeouts
- Log replication to followers
- Heartbeat mechanism for leader liveness
- State machine: Follower → Candidate → Leader

### ACID Properties
- **Atomicity**: Transaction commit/rollback
- **Consistency**: Schema validation
- **Isolation**: Snapshot isolation via MVCC
- **Durability**: WAL with fsync

### Query Language (PyuSQL)
```sql
-- Create a table
CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, age INT);

-- Insert data
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);

-- Query data
SELECT * FROM users WHERE age > 25 ORDER BY name LIMIT 10;

-- Update data
UPDATE users SET age = 31 WHERE id = 1;

-- Delete data
DELETE FROM users WHERE id = 1;

-- Transactions
BEGIN;
INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);
COMMIT;
```

## Quick Start

### Build
```bash
cargo build --release
```

### Run CLI
```bash
cargo run --bin pyudb-cli
```

### Run Tests
```bash
cargo test
```

## Project Structure

```
src/
├── lib.rs              # Library root
├── main.rs             # Server entry point
├── error.rs            # Error types
├── storage/            # Storage engine
│   ├── mod.rs
│   ├── btree.rs        # B-Tree implementation
│   ├── wal.rs          # Write-Ahead Log
│   └── engine.rs       # Storage engine facade
├── raft/               # Raft consensus
│   ├── mod.rs
│   ├── state.rs        # Raft state machine
│   ├── log.rs          # Replicated log
│   ├── election.rs     # Leader election
│   ├── replication.rs  # Log replication
│   ├── node.rs         # Raft node
│   └── rpc.rs          # RPC messages
├── txn/                # Transactions
│   ├── mod.rs
│   ├── manager.rs      # Transaction manager
│   ├── mvcc.rs         # MVCC implementation
│   └── lock.rs         # Lock manager
├── query/              # Query language
│   ├── mod.rs
│   ├── lexer.rs        # Tokenizer
│   ├── ast.rs          # Abstract syntax tree
│   ├── parser.rs       # Parser
│   └── executor.rs     # Query executor
├── network/            # Networking
│   ├── mod.rs
│   ├── server.rs       # gRPC server
│   └── client.rs       # gRPC client
└── bin/
    └── cli.rs          # Interactive CLI
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Client Layer                          │
│                   (CLI / SDK / gRPC Client)                  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                       Query Engine                           │
│              (Lexer → Parser → Executor)                     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Transaction Manager                        │
│                    (MVCC + Lock Manager)                     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     Raft Consensus                           │
│         (Leader Election + Log Replication)                  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     Storage Engine                           │
│                   (B-Tree + WAL)                             │
└─────────────────────────────────────────────────────────────┘
```

## License

MIT