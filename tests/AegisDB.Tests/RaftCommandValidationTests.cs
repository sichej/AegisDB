
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Persistence;

namespace AegisDB.Tests;

public sealed class RaftCommandValidationTests
{
    [Fact]
    public async Task AppendLeaderCommand_InvalidCommands_ShouldNotModifyLog()
    {
        string directory = CreateTestDirectory();

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            string statePath = Path.Combine(directory, "state.json");
            string logPath = Path.Combine(directory, "log.json");

            var leader = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(statePath),
                new RaftLogStore(logPath));

            await ElectLeaderAsync(leader);

            WalRecord[] invalidCommands =
            [
                new(WalOperation.Put, "key", null),
                new(WalOperation.Delete, "key", [1]),
                new(WalOperation.Put, "", [1]),
                new(WalOperation.Delete, "", null),
                new((WalOperation)unchecked((byte)999), "key", [1]),
                new(WalOperation.Put, null!, [1])
            ];

            foreach (WalRecord command in invalidCommands)
            {
                await Assert.ThrowsAsync<ArgumentException>(
                    () => leader.AppendLeaderCommandAsync(command));

                var state = await leader.GetStateSnapshotAsync();

                Assert.Equal(0, state.LastLogIndex);
                Assert.Equal(0, state.CommitIndex);
                Assert.Equal(0, state.LastApplied);
            }

            var recovered = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(statePath),
                new RaftLogStore(logPath));

            Assert.Equal(0, recovered.Log.LastIndex);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task RaftLogStore_InvalidImage_ShouldPreserveExistingLog()
    {
        string directory = CreateTestDirectory();

        try
        {
            string path = Path.Combine(directory, "log.json");
            var store = new RaftLogStore(path);

            var validEntry = new RaftLogEntry(
                Index: 1,
                Term: 1,
                Command: new WalRecord(
                    WalOperation.Put,
                    "user:1",
                    [1, 2, 3]));

            await store.SaveAsync([validEntry]);

            var invalidEntry = new RaftLogEntry(
                Index: 2,
                Term: 1,
                Command: new WalRecord(
                    WalOperation.Put,
                    "user:2",
                    null));

            await Assert.ThrowsAsync<ArgumentException>(
                () => store.SaveAsync(
                    [validEntry, invalidEntry]));

            IReadOnlyList<RaftLogEntry> recovered =
                await store.LoadAsync();

            Assert.Single(recovered);
            Assert.Equal(1, recovered[0].Index);
            Assert.Equal(
                new byte[] { 1, 2, 3 },
                recovered[0].Command.Value);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task AppendLeaderCommand_EmptyPutValue_ShouldBeAccepted()
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

            RaftLogEntry entry =
                await leader.AppendLeaderCommandAsync(
                    new WalRecord(
                        WalOperation.Put,
                        "empty-value",
                        []));

            Assert.Equal(1, entry.Index);
            Assert.Empty(entry.Command.Value!);

            var recovered = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "state.json")),
                new RaftLogStore(
                    Path.Combine(directory, "log.json")));

            Assert.Equal(
                Array.Empty<byte>(),
                recovered.Log.GetEntry(1)!.Command.Value);
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
            "aegisdb-command-validation",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        return directory;
    }
}
