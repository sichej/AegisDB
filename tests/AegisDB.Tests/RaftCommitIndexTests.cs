
using AegisDB.Core.Consensus;
using AegisDB.Core.Persistence;

namespace AegisDB.Tests;

public sealed class RaftCommitIndexTests
{
    private static RaftNode CreateNodeWithLog()
    {
        var cluster = new RaftClusterConfiguration(
            ["node-1", "node-2", "node-3"]);

        var node = new RaftNode("node-1", cluster);

        for (long index = 1; index <= 5; index++)
        {
            node.Log.Append(new RaftLogEntry(
                Index: index,
                Term: 1,
                Command: new WalRecord(
                    WalOperation.Put,
                    $"key-{index}",
                    [1, 2, 3])));
        }

        return node;
    }

    [Fact]
    public async Task AdvanceCommitIndex_ShouldAdvance()
    {
        var node = CreateNodeWithLog();

        await node.AdvanceCommitIndexAsync(3);

        Assert.Equal(3, node.CommitIndex);
        Assert.Equal(5, node.LastLogIndex);
    }

    [Fact]
    public async Task AdvanceCommitIndex_ShouldRejectBackwardMovement()
    {
        var node = CreateNodeWithLog();

        await node.AdvanceCommitIndexAsync(3);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => node.AdvanceCommitIndexAsync(2));

        Assert.Equal(3, node.CommitIndex);
    }

    [Fact]
    public async Task AdvanceCommitIndex_ShouldRejectNonexistentEntries()
    {
        var node = CreateNodeWithLog();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => node.AdvanceCommitIndexAsync(6));

        Assert.Equal(0, node.CommitIndex);
    }
}
