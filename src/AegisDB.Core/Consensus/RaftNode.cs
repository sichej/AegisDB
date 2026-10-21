
using AegisDB.Core.Consensus.Messages;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Storage;

namespace AegisDB.Core.Consensus;

/// <summary>
/// Represents a node participating in the Raft consensus protocol.
///
/// The node maintains its current role, election term, and voting state.
/// Leader election and log replication will be implemented incrementally.
/// </summary>
public sealed class RaftNode
{
    /// <summary>
    /// Unique identifier of this node within the Raft cluster.
    /// </summary>
    public string NodeId { get; }

    /// <summary>
    /// Latest Raft term observed by this node.
    /// </summary>
    public long CurrentTerm { get; private set; }

    /// <summary>
    /// Identifier of the candidate this node voted for
    /// during its current term, or null if it has not voted.
    /// </summary>
    public string? VotedFor { get; private set; }

    /// <summary>
    /// Current role of the node in the consensus protocol.
    /// </summary>
    public RaftNodeState State { get; private set; }

    /// <summary>
    /// Index of the last entry in the local Raft log.
    /// Zero indicates an empty log.
    /// </summary>
    public long LastLogIndex => Log.LastIndex;

    /// <summary>
    /// Term of the last entry in the local Raft log.
    /// Zero indicates an empty log.
    /// </summary>
    public long LastLogTerm => Log.LastTerm;

    private readonly HashSet<string> _votesReceived = new();

    /// <summary>
    /// Number of distinct votes received during the current election,
    /// including the candidate's own vote.
    /// </summary>
    public int VotesReceived => _votesReceived.Count;

    public RaftClusterConfiguration Cluster { get; }

    private readonly RaftStateStore? _stateStore;

    /// <summary>
    /// Serializes Raft state transitions that involve persistent storage.
    /// </summary>
    private readonly SemaphoreSlim _stateLock = new(1, 1);

    public RaftLog Log { get; }

    /// <summary>
    /// Highest log index known to be committed.
    ///
    /// This is volatile Raft state and starts at zero.
    /// Committed entries must never be overwritten.
    /// </summary>
    public long CommitIndex { get; private set; }

    /// <summary>
    /// Highest committed log index applied to the state machine.
    /// </summary>
    public long LastApplied { get; private set; }

    private readonly RaftStateMachine _stateMachine;

    public KeyValueStore Store { get; }


    public RaftNode(
        string nodeId,
        RaftClusterConfiguration cluster,
        RaftStateStore? stateStore = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        ArgumentNullException.ThrowIfNull(cluster);

        if (!cluster.Contains(nodeId))
        {
            throw new ArgumentException(
                "The local node must be a cluster member.",
                nameof(nodeId));
        }

        Log = new RaftLog();

        Store = new KeyValueStore();
        _stateMachine = new RaftStateMachine(Store);

        NodeId = nodeId;
        Cluster = cluster;
        _stateStore = stateStore;

        CommitIndex = 0;
        LastApplied = 0;
        CurrentTerm = 0;
        VotedFor = null;
        State = RaftNodeState.Follower;
    }


    /// <summary>
    /// Starts a new Raft election.
    ///
    /// The node increments its current term, transitions to Candidate,
    /// and votes for itself in the new term.
    ///
    /// Persistent state must be saved before sending RequestVote RPCs.
    /// </summary>
    public void StartElection()
    {
        EnsureInMemoryMode();

        if (State == RaftNodeState.Leader)
        {
            throw new InvalidOperationException(
                "A leader cannot start a new election.");
        }

        CurrentTerm++;
        VotedFor = NodeId;
        State = RaftNodeState.Candidate;

        // Discard votes from previous elections.
        _votesReceived.Clear();

        // Each candidate votes for itself.
        _votesReceived.Add(NodeId);
    }


    /// <summary>
    /// Transitions the node to Follower after observing a newer Raft term.
    ///
    /// A higher term invalidates the previous vote and leadership state.
    /// The updated term and vote must be persisted before processing
    /// protocol messages that depend on them.
    /// </summary>
    public void BecomeFollower(long term)
    {
        EnsureInMemoryMode();

        if (term < CurrentTerm)
        {
            throw new ArgumentOutOfRangeException(
                nameof(term),
                "Cannot transition to an older Raft term.");
        }

        if (term > CurrentTerm)
        {
            CurrentTerm = term;

            // Votes belong to a specific term and cannot
            // be carried over to a newer one.
            VotedFor = null;
        }

        State = RaftNodeState.Follower;

        // Votes collected during a previous candidacy are no longer valid.
        _votesReceived.Clear();
    }


    /// <summary>
    /// Evaluates a RequestVote RPC according to Raft election rules.
    ///
    /// A vote is granted only when the candidate's term is current,
    /// this node has not voted for another candidate in that term,
    /// and the candidate's log is at least as up-to-date as the local log.
    ///
    /// This is an in-memory implementation. Persistent election state
    /// must be introduced before enabling network RPCs.
    /// </summary>
    public RequestVoteResponse HandleRequestVote(RequestVoteRequest request)
    {
        EnsureInMemoryMode();
        ArgumentNullException.ThrowIfNull(request);

        // Reject candidates from older terms.
        if (request.Term < CurrentTerm)
            return new RequestVoteResponse(CurrentTerm, false);

        // A higher term invalidates the previous election state.
        if (request.Term > CurrentTerm)
            BecomeFollower(request.Term);

        bool canVote =
            VotedFor is null ||
            VotedFor == request.CandidateId;

        // Raft compares the last log term first, then the log index.
        bool logIsUpToDate =
            request.LastLogTerm > LastLogTerm ||
            (request.LastLogTerm == LastLogTerm &&
             request.LastLogIndex >= LastLogIndex);

        if (!canVote || !logIsUpToDate)
            return new RequestVoteResponse(CurrentTerm, false);

        VotedFor = request.CandidateId;

        return new RequestVoteResponse(CurrentTerm, true);
    }

    /// <summary>
    /// Transitions the node from Candidate to Leader.
    ///
    /// This method must only be called after the candidate
    /// obtains a majority of votes in its current term.
    /// </summary>
    public void BecomeLeader()
    {
        EnsureInMemoryMode();

        if (State != RaftNodeState.Candidate)
        {
            throw new InvalidOperationException(
                "Only a candidate can become leader.");
        }

        State = RaftNodeState.Leader;
    }



    /// <summary>
    /// Processes a vote response from a configured cluster member.
    ///
    /// Duplicate votes, unknown voters and responses from previous
    /// terms are not counted.
    /// </summary>
    public bool ReceiveVote(
        string voterId,
        long term,
        bool voteGranted)
    {
        EnsureInMemoryMode();
        ArgumentException.ThrowIfNullOrWhiteSpace(voterId);

        // Never count votes from nodes outside this cluster.
        if (!Cluster.Contains(voterId) || voterId == NodeId)
            return false;

        // A higher term invalidates the current candidacy.
        if (term > CurrentTerm)
        {
            BecomeFollower(term);
            return false;
        }

        if (State != RaftNodeState.Candidate ||
            term != CurrentTerm ||
            !voteGranted)
        {
            return false;
        }

        // Each voter contributes at most one vote.
        _votesReceived.Add(voterId);

        if (_votesReceived.Count >= Cluster.QuorumSize)
        {
            BecomeLeader();
            return true;
        }

        return false;
    }


    /// <summary>
    /// Creates a Raft node by restoring its persistent election state.
    ///
    /// A restarted node always begins as a Follower, while its
    /// current term and previous vote are recovered from disk.
    /// </summary>
    public static async Task<RaftNode> CreateAsync(
        string nodeId,
        RaftClusterConfiguration cluster,
        RaftStateStore stateStore,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stateStore);

        var persistedState = await stateStore.LoadAsync(cancellationToken);

        var node = new RaftNode(nodeId, cluster, stateStore)
        {
            CurrentTerm = persistedState.CurrentTerm,
            VotedFor = persistedState.VotedFor
        };

        return node;
    }


    /// <summary>
    /// Starts a new election and durably records the new term
    /// and self-vote before changing the in-memory node state.
    ///
    /// Callers must serialize concurrent Raft operations.
    /// </summary>
    public async Task StartElectionAsync(
        CancellationToken cancellationToken = default)
    {
        if (_stateStore is null)
        {
            throw new InvalidOperationException(
                "Persistent state store is not configured.");
        }

        await _stateLock.WaitAsync(cancellationToken);

        try
        {
            if (State == RaftNodeState.Leader)
            {
                throw new InvalidOperationException(
                    "A leader cannot start a new election.");
            }

            long newTerm = checked(CurrentTerm + 1);

            var persistentState = new RaftPersistentState(
                CurrentTerm: newTerm,
                VotedFor: NodeId
            );

            // Persist before publishing the new election state.
            await _stateStore.SaveAsync(
                persistentState,
                cancellationToken);

            CurrentTerm = newTerm;
            VotedFor = NodeId;
            State = RaftNodeState.Candidate;

            _votesReceived.Clear();
            _votesReceived.Add(NodeId);
        }
        finally
        {
            _stateLock.Release();
        }
    }


    private void EnsureInMemoryMode()
    {
        if (_stateStore is not null)
        {
            throw new InvalidOperationException(
                "This operation is not yet supported for persistent Raft nodes.");
        }
    }


    /// <summary>
    /// Transitions a persistent Raft node to Follower.
    ///
    /// When a newer term is observed, the node durably records
    /// the new term and clears its previous vote before updating
    /// its in-memory state.
    /// </summary>
    public async Task BecomeFollowerAsync(
        long term,
        CancellationToken cancellationToken = default)
    {
        if (_stateStore is null)
        {
            throw new InvalidOperationException(
                "Persistent state store is not configured.");
        }

        await _stateLock.WaitAsync(cancellationToken);

        try
        {
            if (term < CurrentTerm)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(term),
                    "Cannot transition to an older Raft term.");
            }

            if (term > CurrentTerm)
            {
                var persistentState = new RaftPersistentState(
                    CurrentTerm: term,
                    VotedFor: null
                );

                // A newer term must survive a node restart.
                await _stateStore.SaveAsync(
                    persistentState,
                    cancellationToken);

                CurrentTerm = term;
                VotedFor = null;
            }

            State = RaftNodeState.Follower;
            _votesReceived.Clear();
        }
        finally
        {
            _stateLock.Release();
        }
    }


    /// <summary>
    /// Evaluates a RequestVote RPC and persists any changes to
    /// the current term or vote before returning the response.
    ///
    /// Raft state transitions are serialized using the node lock.
    /// </summary>
    public async Task<RequestVoteResponse> HandleRequestVoteAsync(
        RequestVoteRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (string.IsNullOrWhiteSpace(request.CandidateId))
        {
            throw new ArgumentException(
                "Candidate ID cannot be empty.",
                nameof(request));
        }

        if (request.Term < 0 ||
            request.LastLogIndex < 0 ||
            request.LastLogTerm < 0 ||
            (request.LastLogIndex == 0 && request.LastLogTerm != 0))
        {
            throw new ArgumentException(
                "Invalid RequestVote log metadata or term.",
                nameof(request));
        }

        if (_stateStore is null)
        {
            throw new InvalidOperationException(
                "Persistent state store is not configured.");
        }

        await _stateLock.WaitAsync(cancellationToken);

        try
        {
            // Ignore election requests from nodes outside the voting configuration.
            if (!Cluster.Contains(request.CandidateId))
            {
                return new RequestVoteResponse(CurrentTerm, false);
            }

            if (request.Term < CurrentTerm)
            {
                return new RequestVoteResponse(CurrentTerm, false);
            }

            // A higher term invalidates our previous vote and candidacy.
            if (request.Term > CurrentTerm)
            {
                await _stateStore.SaveAsync(
                    new RaftPersistentState(request.Term, null),
                    cancellationToken);

                CurrentTerm = request.Term;
                VotedFor = null;
                State = RaftNodeState.Follower;
                _votesReceived.Clear();
            }

            bool canVote =
                VotedFor is null ||
                VotedFor == request.CandidateId;

            // Compare the candidate's last log term first,
            // then its last log index.
            bool logIsUpToDate =
                request.LastLogTerm > LastLogTerm ||
                (request.LastLogTerm == LastLogTerm &&
                 request.LastLogIndex >= LastLogIndex);

            if (!canVote || !logIsUpToDate)
            {
                return new RequestVoteResponse(CurrentTerm, false);
            }

            // The vote must be durable before it is granted.
            if (VotedFor != request.CandidateId)
            {
                await _stateStore.SaveAsync(
                    new RaftPersistentState(CurrentTerm, request.CandidateId),
                    cancellationToken);

                VotedFor = request.CandidateId;
            }

            return new RequestVoteResponse(CurrentTerm, true);
        }
        finally
        {
            _stateLock.Release();
        }
    }


    /// <summary>
    /// Processes a heartbeat-only AppendEntries RPC.
    ///
    /// Validates the leader's term and local log consistency.
    /// A valid heartbeat causes the node to become a Follower.
    ///
    /// Log replication and commit index updates are not
    /// implemented yet.
    /// </summary>
    public async Task<AppendEntriesResponse> HandleAppendEntriesAsync(
        AppendEntriesRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (_stateStore is null)
        {
            throw new InvalidOperationException(
                "Persistent state store is not configured.");
        }

        if (request.Term < 0 ||
            request.PrevLogIndex < 0 ||
            request.PrevLogTerm < 0 ||
            request.LeaderCommit < 0 ||
            string.IsNullOrWhiteSpace(request.LeaderId) ||
            request.Entries is null ||
            (request.PrevLogIndex == 0 && request.PrevLogTerm != 0))
        {
            throw new ArgumentException(
                "Invalid AppendEntries request.",
                nameof(request));
        }

        if (request.Entries.Count != 0)
        {
            throw new NotSupportedException(
                "Log replication is not implemented yet.");
        }

        await _stateLock.WaitAsync(cancellationToken);

        try
        {
            // Reject messages from nodes outside the voting configuration.
            if (!Cluster.Contains(request.LeaderId))
            {
                return new AppendEntriesResponse(CurrentTerm, false);
            }

            // An outdated leader cannot modify this node.
            if (request.Term < CurrentTerm)
            {
                return new AppendEntriesResponse(CurrentTerm, false);
            }

            if (request.Term > CurrentTerm)
            {
                // A newer term must be persisted before it is accepted.
                await _stateStore.SaveAsync(
                    new RaftPersistentState(
                        request.Term,
                        VotedFor: null),
                    cancellationToken);

                CurrentTerm = request.Term;
                VotedFor = null;
            }

            // An accepted leader message establishes the Follower role.
            State = RaftNodeState.Follower;
            _votesReceived.Clear();

            // Verify that the follower contains the preceding log entry.
            if (!Log.Matches(
                request.PrevLogIndex,
                request.PrevLogTerm))
            {
                return new AppendEntriesResponse(CurrentTerm, false);
            }

            return new AppendEntriesResponse(CurrentTerm, true);
        }
        finally
        {
            _stateLock.Release();
        }
    }


    /// <summary>
    /// Advances the local commit index without allowing it to move backward.
    ///
    /// The caller must establish that the supplied index is committed
    /// according to the Raft protocol before invoking this method.
    /// </summary>
    public async Task AdvanceCommitIndexAsync(
        long newCommitIndex,
        CancellationToken cancellationToken = default)
    {
        await _stateLock.WaitAsync(cancellationToken);

        try
        {
            if (newCommitIndex < CommitIndex)
            {
                throw new InvalidOperationException(
                    "Commit index cannot move backward.");
            }

            if (newCommitIndex > Log.LastIndex)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(newCommitIndex),
                    "Cannot commit entries that do not exist.");
            }

            CommitIndex = newCommitIndex;
        }
        finally
        {
            _stateLock.Release();
        }
    }


    /// <summary>
    /// Records that committed log entries have been applied
    /// to the state machine.
    ///
    /// The caller must apply the corresponding commands successfully
    /// before advancing this index.
    /// </summary>
    public async Task AdvanceLastAppliedAsync(
        long newLastApplied,
        CancellationToken cancellationToken = default)
    {
        await _stateLock.WaitAsync(cancellationToken);

        try
        {
            if (newLastApplied < LastApplied)
            {
                throw new InvalidOperationException(
                    "LastApplied cannot move backward.");
            }

            if (newLastApplied > CommitIndex)
            {
                throw new InvalidOperationException(
                    "Cannot apply entries that are not committed.");
            }

            LastApplied = newLastApplied;
        }
        finally
        {
            _stateLock.Release();
        }
    }


    /// <summary>
    /// Applies all committed but not yet applied Raft log entries
    /// to the local state machine, in log order.
    ///
    /// The node lock prevents concurrent apply operations and
    /// protects the LastApplied progression.
    /// </summary>
    public async Task ApplyCommittedEntriesAsync(
        CancellationToken cancellationToken = default)
    {
        await _stateLock.WaitAsync(cancellationToken);

        try
        {
            while (LastApplied < CommitIndex)
            {
                cancellationToken.ThrowIfCancellationRequested();

                long nextIndex = LastApplied + 1;

                RaftLogEntry entry = Log.GetEntry(nextIndex)
                    ?? throw new InvalidOperationException(
                        $"Missing committed Raft entry at index {nextIndex}.");

                // Apply the operation before advancing LastApplied.
                _stateMachine.Apply(entry);

                LastApplied = nextIndex;
            }
        }
        finally
        {
            _stateLock.Release();
        }
    }



}
