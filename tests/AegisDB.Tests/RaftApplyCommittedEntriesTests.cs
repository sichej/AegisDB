
using System.Text;
using AegisDB.Core.Consensus;
using AegisDB.Core.Persistence;
using AegisDB.Core.Storage;

namespace AegisDB.Tests;

public sealed class RaftApplyCommittedEntriesTests
{
    [Fact]
    public async Task ApplyCommittedEntries_ShouldApplyOnlyCommittedCommands()
    {
        var cluster = new RaftClusterConfiguration(
            ["node-1", "node-2", "node-3"]);

        var store = new KeyValueStore();
        var stateMachine = new RaftStateMachine(store);

        RaftLogEntry[] entries =
        [
            new RaftLogEntry(
                1, 1,
                new WalRecord(
                    WalOperation.Put,
                    "user:1",
                    Encoding.UTF8.GetBytes("Alice"))),
            new RaftLogEntry(
                2, 1,
                new WalRecord(
                    WalOperation.Put,
                    "user:2",
                    Encoding.UTF8.GetBytes("Bob"))),
            new RaftLogEntry(
                3, 1,
                new WalRecord(
                    WalOperation.Delete,
                    "user:1",
                    null)),
            new RaftLogEntry(
                4, 1,
                new WalRecord(
                    WalOperation.Put,
                    "user:3",
                    Encoding.UTF8.GetBytes("Charlie")))
        ];

        var node = new RaftNode(
            "node-1",
            cluster,
            initialEntries: entries,
            stateMachine: stateMachine);

        // The fourth entry has not been committed.
        await node.AdvanceCommitIndexAsync(3);

        await node.ApplyCommittedEntriesAsync();

        Assert.Equal(3, node.LastApplied);
        Assert.Equal(3, node.CommitIndex);

        Assert.Null(store.Get("user:1"));

        Assert.Equal(
            "Bob",
            Encoding.UTF8.GetString(store.Get("user:2")!));

        Assert.Null(store.Get("user:3"));
    }
}
