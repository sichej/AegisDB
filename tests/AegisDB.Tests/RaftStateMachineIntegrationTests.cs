
using AegisDB.Core.Consensus;
using AegisDB.Core.Persistence;
using AegisDB.Tests.Helpers;

namespace AegisDB.Tests;

public sealed class RaftStateMachineIntegrationTests
{
    [Fact]
    public async Task ApplyCommittedEntries_ShouldApplyInOrderOnlyOnce()
    {
        var cluster = new RaftClusterConfiguration(
            ["node-1", "node-2", "node-3"]);

        var stateMachine = new RecordingRaftStateMachine();

        var node = new RaftNode(
            "node-1",
            cluster,
            stateMachine: stateMachine);

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

        // Only the first three entries are committed.
        await node.AdvanceCommitIndexAsync(3);

        await node.ApplyCommittedEntriesAsync();

        Assert.Equal(
            new long[] { 1, 2, 3 },
            stateMachine.AppliedIndices);

        Assert.Equal(3, node.LastApplied);

        // Repeating the apply loop must not execute
        // already applied entries again.
        await node.ApplyCommittedEntriesAsync();

        Assert.Equal(
            new long[] { 1, 2, 3 },
            stateMachine.AppliedIndices);

        // Once more entries become committed,
        // only the missing entries must be applied.
        await node.AdvanceCommitIndexAsync(5);

        await node.ApplyCommittedEntriesAsync();

        Assert.Equal(
            new long[] { 1, 2, 3, 4, 5 },
            stateMachine.AppliedIndices);

        Assert.Equal(5, node.LastApplied);
    }
}
