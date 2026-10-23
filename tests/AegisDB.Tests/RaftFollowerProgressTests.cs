
using AegisDB.Core.Consensus;

namespace AegisDB.Tests;

public sealed class RaftFollowerProgressTests
{
    [Fact]
    public void Constructor_ShouldInitializeProgressFromLeaderLog()
    {
        var progress = new RaftFollowerProgress(
            followerId: "node-2",
            leaderLastLogIndex: 5);

        var snapshot = progress.GetSnapshot();

        Assert.Equal("node-2", progress.FollowerId);
        Assert.Equal(6, snapshot.NextIndex);
        Assert.Equal(0, snapshot.MatchIndex);
    }

    [Fact]
    public void RecordSuccess_ShouldAdvanceMatchAndNextIndex()
    {
        var progress = new RaftFollowerProgress(
            "node-2",
            leaderLastLogIndex: 5);

        progress.RecordSuccess(5);

        var snapshot = progress.GetSnapshot();

        Assert.Equal(5, snapshot.MatchIndex);
        Assert.Equal(6, snapshot.NextIndex);
    }

    [Fact]
    public void RecordSuccess_ShouldNotRegressOnStaleResponse()
    {
        var progress = new RaftFollowerProgress(
            "node-2",
            leaderLastLogIndex: 5);

        progress.RecordSuccess(5);

        // An older successful RPC finishes afterwards.
        progress.RecordSuccess(3);

        var snapshot = progress.GetSnapshot();

        Assert.Equal(5, snapshot.MatchIndex);
        Assert.Equal(6, snapshot.NextIndex);
    }

    [Fact]
    public void RecordFailure_ShouldDecrementNextIndex()
    {
        var progress = new RaftFollowerProgress(
            "node-2",
            leaderLastLogIndex: 5);

        progress.RecordFailure();

        var snapshot = progress.GetSnapshot();

        Assert.Equal(5, snapshot.NextIndex);
        Assert.Equal(0, snapshot.MatchIndex);
    }

    [Fact]
    public void RecordFailure_ShouldNotMoveBehindConfirmedEntries()
    {
        var progress = new RaftFollowerProgress(
            "node-2",
            leaderLastLogIndex: 5);

        progress.RecordSuccess(3);

        // Repeated rejections cannot invalidate a confirmed prefix.
        for (int i = 0; i < 10; i++)
        {
            progress.RecordFailure();
        }

        var snapshot = progress.GetSnapshot();

        Assert.Equal(3, snapshot.MatchIndex);
        Assert.Equal(4, snapshot.NextIndex);
    }

    [Fact]
    public void ConcurrentSuccessResponses_ShouldPreserveHighestIndex()
    {
        var progress = new RaftFollowerProgress(
            "node-2",
            leaderLastLogIndex: 10);

        Parallel.For(
            1,
            11,
            index => progress.RecordSuccess(index));

        var snapshot = progress.GetSnapshot();

        Assert.Equal(10, snapshot.MatchIndex);
        Assert.Equal(11, snapshot.NextIndex);
    }

    [Fact]
    public void Constructor_ShouldRejectNegativeLogIndex()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new RaftFollowerProgress(
                "node-2",
                leaderLastLogIndex: -1));
    }

    [Fact]
    public void RecordSuccess_ShouldRejectNegativeIndex()
    {
        var progress = new RaftFollowerProgress(
            "node-2",
            leaderLastLogIndex: 5);

        Assert.Throws<ArgumentOutOfRangeException>(
            () => progress.RecordSuccess(-1));
    }
}
