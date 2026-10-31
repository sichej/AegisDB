
using AegisDB.Core.Consensus.Messages;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Persistence;
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

    private readonly RaftLog _log;

    public IRaftLogReader Log => _log;

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

    private readonly IRaftStateMachine _stateMachine;

    private readonly RaftLogStore? _logStore;

    private readonly RaftElectionTimer? _electionTimer;

    private int _electionInProgress;


    public RaftNode(
        string nodeId,
        RaftClusterConfiguration cluster,
        RaftStateStore? stateStore = null,
        IRaftStateMachine? stateMachine = null,
        RaftLogStore? logStore = null,
        RaftElectionTimer? electionTimer = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        ArgumentNullException.ThrowIfNull(cluster);

        if (!cluster.Contains(nodeId))
        {
            throw new ArgumentException(
                "The local node must be a cluster member.",
                nameof(nodeId));
        }

        NodeId = nodeId;
        Cluster = cluster;
        _stateStore = stateStore;
        _logStore = logStore;
        _electionTimer = electionTimer;

        _log = new RaftLog();

        _stateMachine = stateMachine
            ?? new RaftStateMachine(new KeyValueStore());

        CurrentTerm = 0;
        VotedFor = null;
        State = RaftNodeState.Follower;

        CommitIndex = 0;
        LastApplied = 0;
    }


    /// <summary>
    /// Creates a node with a predefined log for testing.
    ///
    /// The initial log is loaded before the node is used,
    /// avoiding external mutation of its log after construction.
    /// </summary>
    internal RaftNode(
        string nodeId,
        RaftClusterConfiguration cluster,
        IReadOnlyList<RaftLogEntry> initialEntries,
        IRaftStateMachine? stateMachine = null)
        : this(nodeId, cluster, stateMachine: stateMachine)
    {
        _log.ReplaceAll(initialEntries);
    }


    /// <summary>
    /// Allows only one election coordinator to operate on this node
    /// at a time, even when multiple coordinator instances exist.
    /// </summary>
    internal bool TryBeginElection()
    {
        return Interlocked.CompareExchange(
            ref _electionInProgress,
            1,
            0) == 0;
    }

    /// <summary>
    /// Releases the election guard after the coordinator completes.
    /// </summary>
    internal void EndElection()
    {
        Interlocked.Exchange(ref _electionInProgress, 0);
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
    /// Creates a Raft node and restores its persisted term, vote,
    /// and log entries.
    ///
    /// The node always restarts as a Follower. Committed entries
    /// are not automatically applied during construction.
    /// </summary>
    public static async Task<RaftNode> CreateAsync(
        string nodeId,
        RaftClusterConfiguration cluster,
        RaftStateStore stateStore,
        RaftLogStore? logStore = null,
        CancellationToken cancellationToken = default,
        RaftElectionTimer? electionTimer = null,
        IRaftStateMachine? stateMachine = null)
    {
        ArgumentNullException.ThrowIfNull(stateStore);

        // Load the persistent election state.
        var persistedState = await stateStore.LoadAsync(cancellationToken);

        // Restore the persisted Raft log, if configured.
        IReadOnlyList<RaftLogEntry> persistedEntries = logStore is null
            ? []
            : await logStore.LoadAsync(cancellationToken);

        // Inject the provided state machine or use the default one
        // configured by the RaftNode constructor.
        var node = new RaftNode(
            nodeId,
            cluster,
            stateStore: stateStore,
            logStore: logStore,
            electionTimer: electionTimer,
            stateMachine: stateMachine);

        // Reconstruct the in-memory log from the persisted entries.
        foreach (RaftLogEntry entry in persistedEntries)
        {
            node._log.Append(entry);
        }

        // Restore the persistent term and vote.
        node.CurrentTerm = persistedState.CurrentTerm;
        node.VotedFor = persistedState.VotedFor;

        // Raft nodes restart as Followers, never as Leaders.
        node.State = RaftNodeState.Follower;

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

            // A granted vote resets the election timer.
            _electionTimer?.Reset();

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

            // A valid leader message prevents a new election,
            // even if the subsequent log consistency check fails.
            _electionTimer?.Reset();


            // The follower must contain the entry preceding the batch.
            if (!Log.Matches(
                request.PrevLogIndex,
                request.PrevLogTerm))
            {
                return new AppendEntriesResponse(CurrentTerm, false);
            }

            // Validate the complete batch before modifying the log.
            // The first entry must immediately follow PrevLogIndex.
            long expectedIndex = request.PrevLogIndex;

            foreach (RaftLogEntry entry in request.Entries)
            {
                if (entry is null ||
                    entry.Index != ++expectedIndex ||
                    entry.Term < 0 ||
                    entry.Term > request.Term ||
                    entry.Command is null ||
                    string.IsNullOrEmpty(entry.Command.Key) ||
                    !RaftCommandValidator.IsValid(entry.Command))
                {
                    return new AppendEntriesResponse(CurrentTerm, false);
                }
            }

            // Never allow a conflicting entry to overwrite committed data.
            long? firstConflict = _log.FindFirstConflict(request.Entries);

            if (firstConflict is not null &&
                firstConflict.Value <= CommitIndex)
            {
                return new AppendEntriesResponse(CurrentTerm, false);
            }



            // Persist incoming entries before acknowledging replication.
            if (request.Entries.Count > 0)
            {
                if (_logStore is null)
                {
                    throw new InvalidOperationException(
                        "Persistent Raft log store is not configured.");
                }

                var updatedLog = new RaftLog();
                updatedLog.ReplaceAll(Log.CreateSnapshot());
                updatedLog.AppendEntries(request.Entries);

                var updatedEntries = updatedLog.CreateSnapshot();

                await _logStore.SaveAsync(updatedEntries, cancellationToken);

                _log.ReplaceAll(updatedEntries);
            }

            // The last index covered by this AppendEntries request.
            // For a heartbeat, this equals PrevLogIndex.
            long lastNewEntryIndex = request.PrevLogIndex +
                                     request.Entries.Count;

            // A follower cannot commit entries beyond those confirmed
            // by the successfully processed AppendEntries request.
            long newCommitIndex = Math.Min(
                request.LeaderCommit,
                lastNewEntryIndex);

            if (newCommitIndex > CommitIndex)
            {
                CommitIndex = newCommitIndex;
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


    /// <summary>
    /// Processes a vote response while holding the Raft state lock.
    ///
    /// Only votes from configured peers in the current election
    /// are counted. A higher term causes the node to step down.
    /// </summary>
    public async Task<bool> ReceiveVoteAsync(
        string voterId,
        long term,
        bool voteGranted,
        long electionTerm,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(voterId);

        await _stateLock.WaitAsync(cancellationToken);

        try
        {
            if (!Cluster.Contains(voterId) || voterId == NodeId)
                return false;

            if (term > CurrentTerm)
            {
                if (_stateStore is not null)
                {
                    await _stateStore.SaveAsync(
                        new RaftPersistentState(term, null),
                        cancellationToken);
                }

                CurrentTerm = term;
                VotedFor = null;
                State = RaftNodeState.Follower;
                _votesReceived.Clear();

                return false;
            }

            // Ignore responses from previous elections.
            if (State != RaftNodeState.Candidate ||
                CurrentTerm != electionTerm ||
                term != electionTerm ||
                !voteGranted)
            {
                return false;
            }

            _votesReceived.Add(voterId);

            if (_votesReceived.Count >= Cluster.QuorumSize)
            {
                State = RaftNodeState.Leader;
                return true;
            }

            return false;
        }
        finally
        {
            _stateLock.Release();
        }
    }


    /// <summary>
    /// Elects the local candidate when it is the only voting member.
    /// </summary>
    public async Task ReceiveSingleNodeMajorityAsync(
        CancellationToken cancellationToken = default)
    {
        await _stateLock.WaitAsync(cancellationToken);

        try
        {
            if (Cluster.QuorumSize != 1 ||
                State != RaftNodeState.Candidate)
            {
                throw new InvalidOperationException(
                    "Single-node majority is not applicable.");
            }

            State = RaftNodeState.Leader;
        }
        finally
        {
            _stateLock.Release();
        }
    }


    /// <summary>
    /// Starts an election only if no heartbeat or granted vote
    /// has reset the election timer since the observed generation.
    ///
    /// The generation check and state transition are coordinated
    /// with the Raft node's state lock.
    /// </summary>
    public async Task<bool> TryStartElectionAsync(
        long expectedGeneration,
        CancellationToken cancellationToken = default)
    {
        if (_stateStore is null || _electionTimer is null)
        {
            throw new InvalidOperationException(
                "Persistent state and election timer are required.");
        }

        await _stateLock.WaitAsync(cancellationToken);

        try
        {
            if (State == RaftNodeState.Leader ||
                _electionTimer.Generation != expectedGeneration)
            {
                return false;
            }

            long nextTerm = checked(CurrentTerm + 1);

            await _stateStore.SaveAsync(
                new RaftPersistentState(nextTerm, NodeId),
                cancellationToken);

            CurrentTerm = nextTerm;
            VotedFor = NodeId;
            State = RaftNodeState.Candidate;

            _votesReceived.Clear();
            _votesReceived.Add(NodeId);

            return true;
        }
        finally
        {
            _stateLock.Release();
        }
    }


    /// <summary>
    /// Returns a consistent snapshot of the node's consensus state.
    ///
    /// Acquires the state lock to prevent observing partially
    /// completed state transitions.
    /// </summary>
    public async Task<RaftNodeStateSnapshot> GetStateSnapshotAsync(
        CancellationToken cancellationToken = default)
    {
        await _stateLock.WaitAsync(cancellationToken);

        try
        {
            return new RaftNodeStateSnapshot(
                NodeId: NodeId,
                State: State,
                CurrentTerm: CurrentTerm,
                VotedFor: VotedFor,
                CommitIndex: CommitIndex,
                LastApplied: LastApplied,
                LastLogIndex: _log.LastIndex,
                LastLogTerm: _log.LastTerm);
        }
        finally
        {
            _stateLock.Release();
        }
    }


    /// <summary>
    /// Steps down only when a response reveals a term strictly
    /// greater than the node's current term.
    ///
    /// The comparison and transition use the same state lock,
    /// preventing delayed RPC responses from demoting a newer leader.
    /// </summary>
    public async Task<bool> StepDownIfHigherTermAsync(
        long observedTerm,
        CancellationToken cancellationToken = default)
    {
        await _stateLock.WaitAsync(cancellationToken);

        try
        {
            if (observedTerm <= CurrentTerm)
                return false;

            // Persist the new term before publishing the transition.
            if (_stateStore is not null)
            {
                await _stateStore.SaveAsync(
                    new RaftPersistentState(observedTerm, null),
                    cancellationToken);
            }

            CurrentTerm = observedTerm;
            VotedFor = null;
            State = RaftNodeState.Follower;
            _votesReceived.Clear();

            return true;
        }
        finally
        {
            _stateLock.Release();
        }
    }


    /// <summary>
    /// Advances the leader's commit index using confirmed follower
    /// replication progress.
    ///
    /// Leadership, term, log and commitment are validated under
    /// the node state lock before publishing the new commit index.
    /// </summary>
    public async Task<bool> TryAdvanceLeaderCommitAsync(
        long expectedTerm,
        IReadOnlyCollection<long> followerMatchIndexes,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(followerMatchIndexes);

        await _stateLock.WaitAsync(cancellationToken);

        try
        {
            if (State != RaftNodeState.Leader ||
                CurrentTerm != expectedTerm)
            {
                return false;
            }

            if (followerMatchIndexes.Count !=
                Cluster.ClusterSize - 1)
            {
                throw new ArgumentException(
                    "Exactly one match index is required per follower.",
                    nameof(followerMatchIndexes));
            }

            long lastIndex = _log.LastIndex;

            long newCommitIndex =
                RaftCommitCalculator.CalculateCommitIndex(
                    currentCommitIndex: CommitIndex,
                    currentTerm: CurrentTerm,
                    leaderLastLogIndex: lastIndex,
                    quorumSize: Cluster.QuorumSize,
                    followerMatchIndexes: followerMatchIndexes,
                    getEntryTerm: index =>
                        _log.GetEntry(index)?.Term
                        ?? throw new InvalidOperationException(
                            $"Log entry {index} does not exist."));

            if (newCommitIndex <= CommitIndex)
                return false;

            CommitIndex = newCommitIndex;

            return true;
        }
        finally
        {
            _stateLock.Release();
        }
    }


    /// <summary>
    /// Appends a client command to the leader's persistent Raft log.
    ///
    /// The command is durable locally before becoming visible in
    /// the in-memory log. It is not committed by this operation.
    /// </summary>
    public async Task<RaftLogEntry> AppendLeaderCommandAsync(
        WalRecord command,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(command);

        // Take ownership of the caller's mutable value buffer
        // before validation or asynchronous operations.
        var ownedCommand = new WalRecord(
            command.Operation,
            command.Key,
            command.Value?.ToArray());

        // Reject invalid commands before entering the state lock
        // or performing any persistent operation.
        RaftCommandValidator.Validate(ownedCommand);

        await _stateLock.WaitAsync(cancellationToken);

        try
        {
            if (State != RaftNodeState.Leader)
            {
                throw new InvalidOperationException(
                    "Only the current Raft leader can append client commands.");
            }

            if (_logStore is null)
            {
                throw new InvalidOperationException(
                    "Persistent Raft log storage is not configured.");
            }

            long nextIndex = checked(_log.LastIndex + 1);

            var entry = new RaftLogEntry(
                Index: nextIndex,
                Term: CurrentTerm,
                Command: ownedCommand);

            // Build the new log without modifying the published log.
            var updatedLog = new RaftLog();

            updatedLog.ReplaceAll(_log.CreateSnapshot());
            updatedLog.Append(entry);

            IReadOnlyList<RaftLogEntry> updatedEntries =
                updatedLog.CreateSnapshot();

            // Persist before publishing the new entry.
            await _logStore.SaveAsync(
                updatedEntries,
                cancellationToken);

            // Persistence succeeded. Publish atomically in memory.
            _log.ReplaceAll(updatedEntries);

            return entry;
        }
        finally
        {
            _stateLock.Release();
        }
    }


}
