
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Persistence;

namespace AegisDB.Tests;

public sealed class RaftLeaderCommandTests
{
    [Fact]
    public async Task AppendLeaderCommand_ShouldPersistBeforeReturning()
    {
        string directory = CreateTestDirectory();

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var stateStore = new RaftStateStore(
                Path.Combine(directory, "state.json"));

            var logStore = new RaftLogStore(
                Path.Combine(directory, "log.json"));

            var leader = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                stateStore,
                logStore);

            await ElectLeaderAsync(leader);

            var command = new WalRecord(
                WalOperation.Put,
                "user:1",
                [65, 108, 105, 99, 101]);

            RaftLogEntry entry =
                await leader.AppendLeaderCommandAsync(command);

            Assert.Equal(1, entry.Index);
            Assert.Equal(1, entry.Term);
            Assert.Equal("user:1", entry.Command.Key);

            var snapshot = await leader.GetStateSnapshotAsync();

            Assert.Equal(1, snapshot.LastLogIndex);

            // Appending locally must not commit the entry.
            Assert.Equal(0, snapshot.CommitIndex);
            Assert.Equal(0, snapshot.LastApplied);

            // Reconstruct the node using the same persistent files.
            var recovered = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "state.json")),
                new RaftLogStore(
                    Path.Combine(directory, "log.json")));

            RaftLogEntry? recoveredEntry =
                recovered.Log.GetEntry(1);

            Assert.NotNull(recoveredEntry);
            Assert.Equal(1, recoveredEntry.Index);
            Assert.Equal(1, recoveredEntry.Term);
            Assert.Equal("user:1", recoveredEntry.Command.Key);

            Assert.Equal(
                new byte[] { 65, 108, 105, 99, 101 },
                recoveredEntry.Command.Value);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task AppendLeaderCommand_ShouldAssignSequentialIndexes()
    {
        string directory = CreateTestDirectory();

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var leader = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "state.json")),
                new RaftLogStore(
                    Path.Combine(directory, "log.json")));

            await ElectLeaderAsync(leader);

            var first = await leader.AppendLeaderCommandAsync(
                new WalRecord(WalOperation.Put, "key-1", [1]));

            var second = await leader.AppendLeaderCommandAsync(
                new WalRecord(WalOperation.Put, "key-2", [2]));

            var third = await leader.AppendLeaderCommandAsync(
                new WalRecord(WalOperation.Delete, "key-1", null));

            Assert.Equal(1, first.Index);
            Assert.Equal(2, second.Index);
            Assert.Equal(3, third.Index);

            Assert.Equal(1, first.Term);
            Assert.Equal(1, second.Term);
            Assert.Equal(1, third.Term);

            var snapshot = await leader.GetStateSnapshotAsync();

            Assert.Equal(3, snapshot.LastLogIndex);
            Assert.Equal(0, snapshot.CommitIndex);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task AppendLeaderCommand_ShouldRejectFollower()
    {
        string directory = CreateTestDirectory();

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var follower = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "state.json")),
                new RaftLogStore(
                    Path.Combine(directory, "log.json")));

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => follower.AppendLeaderCommandAsync(
                    new WalRecord(
                        WalOperation.Put,
                        "key",
                        [1])));

            var snapshot = await follower.GetStateSnapshotAsync();

            Assert.Equal(0, snapshot.LastLogIndex);
            Assert.Equal(0, snapshot.CommitIndex);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task AppendLeaderCommand_ShouldCopyInputValue()
    {
        string directory = CreateTestDirectory();

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var leader = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "state.json")),
                new RaftLogStore(
                    Path.Combine(directory, "log.json")));

            await ElectLeaderAsync(leader);

            byte[] value = [10, 20, 30];

            await leader.AppendLeaderCommandAsync(
                new WalRecord(
                    WalOperation.Put,
                    "key",
                    value));

            // Mutating the caller's buffer must not alter the log.
            value[0] = 99;

            RaftLogEntry? stored = leader.Log.GetEntry(1);

            Assert.NotNull(stored);
            Assert.Equal(
                new byte[] { 10, 20, 30 },
                stored.Command.Value);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static async Task ElectLeaderAsync(RaftNode node)
    {
        await node.StartElectionAsync();

        await node.ReceiveVoteAsync(
            voterId: "node-2",
            term: node.CurrentTerm,
            voteGranted: true,
            electionTerm: node.CurrentTerm);

        Assert.Equal(RaftNodeState.Leader, node.State);
    }

    private static string CreateTestDirectory()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-leader-commands",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        return directory;
    }
}
