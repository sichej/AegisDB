# AegisDB

**A distributed key-value database built from scratch in C# and .NET 10.**

AegisDB is an educational database engineering project focused on storage durability, crash recovery, concurrency, and distributed consensus.

The goal is to design and implement the core mechanisms of a distributed database, rather than relying on an existing database engine.

> **Project status:** Active development. Single-node storage and recovery are implemented; Raft consensus and distributed replication are in progress. Not production-ready.

## Architecture

AegisDB is organized into two main components:

- **AegisDB.Core** — Storage engine, persistence, recovery, and Raft consensus implementation.
- **AegisDB.Server** — HTTP API exposing database operations.
- **dashboard** — React/TypeScript monitoring and distributed key-value explorer for an opt-in local Raft cluster.

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
├── dashboard/
├── .github/
│   └── workflows/
├── AegisDB.slnx
└── README.md
```

## Getting Started

### Requirements

- .NET 10 SDK
- Node.js 22.12+ and npm (for the dashboard)

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

## Local Cluster Dashboard

The server normally hosts **no Raft nodes**: `/kv` continues to use the existing
single-node database engine, WAL, snapshots, and recovery.

For local monitoring, opt into three real Raft nodes hosted in one server process:

```bash
Raft__Enabled=true dotnet run --project src/AegisDB.Server --launch-profile http
```

This starts the backend at `http://localhost:5140`. The cluster uses the existing
`RaftNodeRuntime`, elections, and replication services with one shared
`InMemoryRaftTransport`. All nodes are registered before their runtimes start.
The host also runs the existing state-machine application service for every node.
It cancels and joins both runtimes and application tasks on shutdown; a component failure is logged,
cancels its siblings, and fails the hosted service instead of silently restarting.
The in-process mode is accepted only in Development or Testing and is disabled
by default. Do not start two server instances using the same storage directory.

Raft term/vote metadata and logs are stored separately under
`src/AegisDB.Server/data/raft` when using the command above. Override this with
`Raft__DataDirectory=/absolute/path/to/raft-data`. Recovery uses the existing
Raft implementation: commit/application indices are volatile, and this MVP does
not add state-machine snapshot recovery.

In another terminal:

```bash
cd dashboard
npm ci
npm run dev
```

Open **http://127.0.0.1:5173**. Vite proxies `/admin` to
`http://127.0.0.1:5140`, so no permissive CORS configuration is required.
Both the development server and monitoring access are intended for loopback use.
Do not publish this development proxy or forward public requests to the admin
routes; loopback restriction is not authentication for a public reverse proxy.

### Monitoring API

| Endpoint | Response |
|---|---|
| `GET /admin/cluster` | Hosting mode, observation time, scope description, and local nodes |
| `GET /admin/nodes` | All local node observations |
| `GET /admin/nodes/{id}` | One local node, or 404 Problem Details |

Each node reports its actual ID, role, current term, commit index, last applied
index, last log index/term, and local runtime availability. State comes from
`GetStateSnapshotAsync()`. Terms and indices are JSON decimal strings to avoid
losing 64-bit integer precision in JavaScript. Availability means the local
runtime task is starting/running/stopped/faulted, **not network reachability,
quorum health, or a distributed health check**. Snapshots are consistent per
node, not atomic across the cluster.

The dashboard polls after each request with an approximately one-second interval,
times out stalled requests, retries failures, and labels retained observations as
stale while disconnected. When the cluster is disabled, the API returns no nodes
and the dashboard explains how to enable it rather than displaying sample data.

All `/admin` endpoints are mapped only in Development or Testing and, including
the existing mutation endpoint `POST /admin/snapshot`, reject non-loopback clients. The monitoring endpoints are
read-only; no node stop/restart, fault injection, or simulation control API is
provided.

### Distributed Key-Value Explorer

Below the overview, enter a key (for example `user:1`) and a text or JSON value
(for example `{"name":"Alice"}`). PUT and DELETE submit real commands through
`RaftCommandSubmissionService`; they never mutate a store directly. A successful
response confirms quorum commitment and application on the submitting leader,
and includes its ID, log index, and term. Followers apply committed entries in
their supervised background services. Successful writes immediately refresh
monitoring; regular polling continues afterward.

| Endpoint | Request / result |
|---|---|
| `PUT /admin/kv/{key}` | JSON `{"value":"text or JSON text"}`; committed/applied receipt |
| `GET /admin/kv/{key}` | Value and observed leader metadata, or 404 Problem Details |
| `DELETE /admin/kv/{key}` | Committed/applied receipt, including when the key is absent |

An empty string is a valid value. JSON mode validates the editor contents before
PUT; JSON is stored as text, not interpreted by the database. Keys must contain
non-whitespace characters and fit in 1024 UTF-8 bytes; values are limited to
1 MiB of UTF-8 data.

**Read guarantee:** GET reads the actual leader's locally applied state and reports
`consistency: "leader-local-applied"` and `linearizable: false`. There is no
ReadIndex, leader lease, quorum read barrier, or read-your-writes guarantee across
leadership changes. A partitioned former leader can return stale data. Leadership
is checked locally, not certified by a quorum. `lastApplied` is observed metadata,
not a barrier atomically tied to the returned value. After restart, persisted logs
alone do not reconstruct the state machine until their entries become committed
and applied again (for example through a subsequent current-term committed write).

Writes have a five-second confirmation deadline. No known unique leader returns
503; a detected leadership change returns 409; an unconfirmed write/no quorum
returns 504. Request cancellation is reported as 408 when the client connection
still permits a response. Persistence/replication errors return logged, sanitized
500 Problem Details. **Timeout, cancellation, or a failed response is not rollback:**
an appended command may still commit later. Errors expose an `outcome` and the UI
warns about unknown outcomes. Neither the server nor the dashboard automatically
resubmits client commands; internal Raft retries replicate the same log entry.
Do not blindly repeat an ambiguous write: client request deduplication is not
implemented. These development-only endpoints are not a public management API.

### MVP boundaries

- The three nodes run real Raft but share one process and failure domain.
- `/kv` writes **do not enter the Raft logs**. Use the Explorer or `/admin/kv`
  for distributed operations. The dashboard's indices remain zero until commands
  are submitted through the consensus layer; no background
  demo traffic or fabricated progress is generated.
- Raft persistence is independent of the single-node database storage.
- Multi-process clusters and HTTP/gRPC Raft transport are not yet supported.
  Hosting lives in the Server project, monitoring depends on `IClusterMonitor`,
  and the consensus runtime already accepts `IRaftTransport`, so a future host
  or transport can be added without dashboard-specific consensus changes.
- `npm run build` produces static assets in `dashboard/dist`; deployment behind
  an authenticated gateway is outside this local-only MVP. Vite preview serves
  the assets but is not configured as a monitoring API gateway.

### Dashboard validation

```bash
dotnet test AegisDB.slnx
cd dashboard
npm run typecheck
npm test
npm run build
```

The backend tests use real local HTTP listeners and hosted Raft nodes; they cover
status responses, persisted-log observations, disabled mode, access restrictions,
missing nodes, and shutdown. Frontend tests cover response validation and
64-bit-safe progress calculations.

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