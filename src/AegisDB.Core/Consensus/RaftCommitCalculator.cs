
namespace AegisDB.Core.Consensus;

/// <summary>
/// Computes the highest Raft log index eligible for commitment.
///
/// Only entries from the leader's current term may be committed
/// directly through majority replication.
/// </summary>
public static class RaftCommitCalculator
{
    public static long CalculateCommitIndex(
        long currentCommitIndex,
        long currentTerm,
        long leaderLastLogIndex,
        int quorumSize,
        IReadOnlyCollection<long> followerMatchIndexes,
        Func<long, long> getEntryTerm)
    {
        ArgumentNullException.ThrowIfNull(followerMatchIndexes);
        ArgumentNullException.ThrowIfNull(getEntryTerm);

        if (currentCommitIndex < 0 ||
            currentTerm < 0 ||
            leaderLastLogIndex < currentCommitIndex)
        {
            throw new ArgumentOutOfRangeException(
                nameof(currentCommitIndex));
        }

        if (quorumSize < 1 ||
            quorumSize > followerMatchIndexes.Count + 1)
        {
            throw new ArgumentOutOfRangeException(nameof(quorumSize));
        }

        if (followerMatchIndexes.Any(
            index => index < 0 || index > leaderLastLogIndex))
        {
            throw new ArgumentOutOfRangeException(
                nameof(followerMatchIndexes));
        }

        // The leader always has its own log entries.
        long[] matchIndexes = followerMatchIndexes
            .Append(leaderLastLogIndex)
            .OrderDescending()
            .ToArray();

        // The quorum-th highest matchIndex is the highest index
        // replicated on at least quorumSize members.
        long majorityIndex = matchIndexes[quorumSize - 1];

        // Never move the commit index backwards.
        if (majorityIndex <= currentCommitIndex)
            return currentCommitIndex;

        // Raft only commits entries from the current term
        // directly through a majority-counting rule.
        for (long index = majorityIndex;
             index > currentCommitIndex;
             index--)
        {
            if (getEntryTerm(index) == currentTerm)
                return index;
        }

        return currentCommitIndex;
    }
}
