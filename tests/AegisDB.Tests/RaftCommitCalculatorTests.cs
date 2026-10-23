
using AegisDB.Core.Consensus;

namespace AegisDB.Tests;

public sealed class RaftCommitCalculatorTests
{
    [Fact]
    public void CalculateCommitIndex_ShouldCommitCurrentTermEntryWithMajority()
    {
        // Five nodes: leader + four followers.
        // Three replicas are required for a quorum.
        long[] followerMatches = [8, 7, 5, 3];

        long result = RaftCommitCalculator.CalculateCommitIndex(
            currentCommitIndex: 5,
            currentTerm: 4,
            leaderLastLogIndex: 8,
            quorumSize: 3,
            followerMatchIndexes: followerMatches,
            getEntryTerm: index => index == 8 ? 4 : 3);

        // Index 7 has quorum, but belongs to an older term.
        Assert.Equal(5, result);
    }

    [Fact]
    public void CalculateCommitIndex_ShouldCommitHighestCurrentTermEntry()
    {
        long[] followerMatches = [8, 7, 5, 3];

        long result = RaftCommitCalculator.CalculateCommitIndex(
            currentCommitIndex: 5,
            currentTerm: 4,
            leaderLastLogIndex: 8,
            quorumSize: 3,
            followerMatchIndexes: followerMatches,
            getEntryTerm: index => index >= 7 ? 4 : 3);

        Assert.Equal(7, result);
    }

    [Fact]
    public void CalculateCommitIndex_ShouldNotCommitWithoutQuorum()
    {
        // Only the leader contains index 5.
        long[] followerMatches = [3, 2];

        long result = RaftCommitCalculator.CalculateCommitIndex(
            currentCommitIndex: 3,
            currentTerm: 2,
            leaderLastLogIndex: 5,
            quorumSize: 2,
            followerMatchIndexes: followerMatches,
            getEntryTerm: _ => 2);

        Assert.Equal(3, result);
    }

    [Fact]
    public void CalculateCommitIndex_ShouldCommitOlderEntriesIndirectly()
    {
        // Index 4 belongs to term 2.
        // Index 5 belongs to current term 3.
        // A quorum has replicated through index 5.
        long[] followerMatches = [5, 5];

        long result = RaftCommitCalculator.CalculateCommitIndex(
            currentCommitIndex: 2,
            currentTerm: 3,
            leaderLastLogIndex: 5,
            quorumSize: 2,
            followerMatchIndexes: followerMatches,
            getEntryTerm: index => index == 5 ? 3 : 2);

        // Committing index 5 also commits the preceding entries.
        Assert.Equal(5, result);
    }

    [Fact]
    public void CalculateCommitIndex_ShouldNeverDecreaseCommitIndex()
    {
        long[] followerMatches = [3, 2];

        long result = RaftCommitCalculator.CalculateCommitIndex(
            currentCommitIndex: 5,
            currentTerm: 4,
            leaderLastLogIndex: 6,
            quorumSize: 2,
            followerMatchIndexes: followerMatches,
            getEntryTerm: _ => 4);

        Assert.Equal(5, result);
    }

    [Fact]
    public void CalculateCommitIndex_ShouldSupportSingleNodeCluster()
    {
        long result = RaftCommitCalculator.CalculateCommitIndex(
            currentCommitIndex: 0,
            currentTerm: 1,
            leaderLastLogIndex: 3,
            quorumSize: 1,
            followerMatchIndexes: Array.Empty<long>(),
            getEntryTerm: _ => 1);

        Assert.Equal(3, result);
    }

    [Fact]
    public void CalculateCommitIndex_ShouldRejectInvalidMatchIndex()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            RaftCommitCalculator.CalculateCommitIndex(
                currentCommitIndex: 0,
                currentTerm: 2,
                leaderLastLogIndex: 5,
                quorumSize: 2,
                followerMatchIndexes: new long[] { 6, 3 },
                getEntryTerm: _ => 2));
    }
}
