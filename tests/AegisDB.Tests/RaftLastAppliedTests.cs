
using AegisDB.Core.Consensus;
using AegisDB.Core.Persistence;

namespace AegisDB.Tests;

public sealed class RaftLastAppliedTests
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
    public async Task LastApplied_ShouldAdvanceWithinCommitIndex()
    {
        var node = CreateNodeWithLog();

        await node.AdvanceCommitIndexAsync(3);
        await node.AdvanceLastAppliedAsync(2);

        Assert.Equal(3, node.CommitIndex);
        Assert.Equal(2, node.LastApplied);
    }

    [Fact]
    public async Task LastApplied_ShouldNotExceedCommitIndex()
    {
        var node = CreateNodeWithLog();

        await node.AdvanceCommitIndexAsync(3);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => node.AdvanceLastAppliedAsync(4));

        Assert.Equal(0, node.LastApplied);
    }

    [Fact]
    public async Task LastApplied_ShouldNotMoveBackward()
    {
        var node = CreateNodeWithLog();

        await node.AdvanceCommitIndexAsync(4);
        await node.AdvanceLastAppliedAsync(3);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => node.AdvanceLastAppliedAsync(2));

        Assert.Equal(3, node.LastApplied);
    }
}
