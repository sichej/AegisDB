
using AegisDB.Core.Consensus.Messages;

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
    public long LastLogIndex { get; private set; }

    /// <summary>
    /// Term of the last entry in the local Raft log.
    /// Zero indicates an empty log.
    /// </summary>
    public long LastLogTerm { get; private set; }

    private readonly HashSet<string> _votesReceived = new();

    /// <summary>
    /// Number of distinct votes received during the current election,
    /// including the candidate's own vote.
    /// </summary>
    public int VotesReceived => _votesReceived.Count;

    public RaftNode(string nodeId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);

        NodeId = nodeId;
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
    /// Updates the local log metadata.
    ///
    /// Temporary helper used until RaftNode is connected
    /// to the persistent Raft log.
    /// </summary>
    public void UpdateLastLogInfo(long index, long term)
    {
        if (index < 0 || term < 0 || (index == 0 && term != 0))
            throw new ArgumentOutOfRangeException(nameof(index));

        LastLogIndex = index;
        LastLogTerm = term;
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
        if (State != RaftNodeState.Candidate)
        {
            throw new InvalidOperationException(
                "Only a candidate can become leader.");
        }

        State = RaftNodeState.Leader;
    }


    /// <summary>
    /// Records a vote granted by another cluster node.
    ///
    /// Duplicate votes are ignored. A candidate becomes Leader
    /// after obtaining a majority in its current term.
    ///
    /// The caller must associate the response with the current
    /// election and verify the identity of the responding peer.
    /// </summary>
    public bool ReceiveVote(
        string voterId,
        long term,
        bool voteGranted,
        int clusterSize)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(voterId);

        if (clusterSize < 1)
            throw new ArgumentOutOfRangeException(nameof(clusterSize));

        // A response from a newer term invalidates our candidacy.
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

        // A HashSet prevents counting the same voter twice.
        _votesReceived.Add(voterId);

        int quorum = (clusterSize / 2) + 1;

        if (_votesReceived.Count >= quorum)
        {
            BecomeLeader();
            return true;
        }

        return false;
    }


}
