
namespace AegisDB.Core.Consensus;

/// <summary>
/// Tracks the leader's replication progress for one follower.
///
/// This is volatile, leader-local state. It is reinitialized
/// whenever the node becomes leader.
/// </summary>
public sealed class RaftFollowerProgress
{
    private readonly object _lock = new();

    private long _nextIndex;
    private long _matchIndex;

    public string FollowerId { get; }

    public RaftFollowerProgress(
        string followerId,
        long leaderLastLogIndex)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(followerId);

        if (leaderLastLogIndex < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(leaderLastLogIndex));
        }

        FollowerId = followerId;

        // Raft initializes nextIndex to leader.lastLogIndex + 1.
        _nextIndex = checked(leaderLastLogIndex + 1);

        // No replicated entries have been confirmed yet.
        _matchIndex = 0;
    }

    public (long NextIndex, long MatchIndex) GetSnapshot()
    {
        lock (_lock)
        {
            return (_nextIndex, _matchIndex);
        }
    }

    /// <summary>
    /// Records a successful AppendEntries response.
    ///
    /// The caller must supply the highest index confirmed
    /// by the successful request, not the follower's LastIndex.
    /// </summary>
    public void RecordSuccess(long replicatedThroughIndex)
    {
        if (replicatedThroughIndex < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(replicatedThroughIndex));
        }

        lock (_lock)
        {
            // Delayed responses must not move progress backwards.
            _matchIndex = Math.Max(
                _matchIndex,
                replicatedThroughIndex);

            _nextIndex = Math.Max(
                _nextIndex,
                checked(_matchIndex + 1));
        }
    }

    /// <summary>
    /// Moves nextIndex backwards after an AppendEntries rejection,
    /// without moving behind already confirmed entries.
    ///
    /// This simple retry strategy will later be replaced or
    /// optimized using conflict hints from the follower.
    /// </summary>
    public void RecordFailure()
    {
        lock (_lock)
        {
            _nextIndex = Math.Max(
                _matchIndex + 1,
                _nextIndex - 1);
        }
    }
}
