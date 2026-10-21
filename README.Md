# AegisDB

**A distributed key-value database built from scratch in C# and .NET 10.**

AegisDB is an educational database engineering project focused on storage durability, crash recovery, concurrency, and distributed consensus.

The goal is to design and implement the core mechanisms of a distributed database, rather than relying on an existing database engine.

> **Project status:** Active development. Single-node storage and recovery are implemented; Raft consensus and distributed replication are in progress. Not production-ready.

## Architecture

AegisDB is organized into two main components:

- **AegisDB.Core** — Storage engine, persistence, recovery, and Raft consensus implementation.
- **AegisDB.Server** — HTTP API exposing database operations.

### Single-node storage

```text
HTTP API
   |
DatabaseEngine
   |
   +-- Write-Ahead Log (WAL)
   |
   +-- KeyValueStore
   |
   +-- Snapshots & Recovery
```

Mutations are written to the WAL before they are applied to the in-memory key-value store.

### Distributed architecture (in progress)

```text
          Client
             |
        Raft Leader
             |
       Replicated Log
        /         \
   Follower     Follower
        \         /
       State Machines
             |
        KeyValueStore
```

Raft separates log replication, commitment, and application to the state machine.

## Features

### Implemented

- Thread-safe in-memory key-value storage
- Write-ahead logging with CRC32 integrity checks
- Recovery from WAL records and incomplete trailing writes
- Snapshot creation and restoration
- HTTP endpoints for key-value operations
- Crash recovery and fault-injection testing
- Raft election state and vote handling
- Raft log reconciliation and conflict detection
- Commit and application index tracking

### In progress

- Durable Raft log integration
- Leader election over network RPCs
- Distributed log replication
- Commit propagation and state machine recovery
- Multi-node cluster operation

## Technology Stack

- C# / .NET 10
- ASP.NET Core
- xUnit
- GitHub Actions

## Repository Structure

```text
AegisDB/
├── src/
│   ├── AegisDB.Core/
│   │   ├── Consensus/
│   │   ├── Diagnostics/
│   │   ├── Persistence/
│   │   └── Storage/
│   └── AegisDB.Server/
├── tests/
│   └── AegisDB.Tests/
├── .github/
│   └── workflows/
├── AegisDB.slnx
└── README.md
```

## Getting Started

### Requirements

- .NET 10 SDK

### Build

```bash
dotnet restore AegisDB.slnx
dotnet build AegisDB.slnx
```

### Run

```bash
dotnet run --project src/AegisDB.Server
```

### Test

```bash
dotnet test AegisDB.slnx
```

## Engineering Goals

AegisDB explores several core database engineering challenges:

1. **Durability:** Preserving committed data across crashes and restarts.
2. **Consistency:** Maintaining an agreed operation order across nodes.
3. **Recovery:** Reconstructing database state from persistent logs and snapshots.
4. **Concurrency:** Coordinating operations safely without corrupting shared state.
5. **Fault tolerance:** Handling partial writes, unavailable nodes, and interrupted operations.

## Roadmap

- [x] In-memory key-value store
- [x] Write-ahead logging
- [x] Crash recovery
- [x] Snapshot persistence
- [x] Raft data structures and election logic
- [ ] Persistent Raft log integration
- [ ] Network-based leader election
- [ ] Multi-node log replication
- [ ] Raft commit and state machine integration
- [ ] Distributed recovery and fault tolerance testing
- [ ] Performance benchmarks

## License

License to be determined.