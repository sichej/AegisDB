
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Persistence;

namespace AegisDB.Tests;

public sealed class RaftLogRecoveryTests
{
    [Fact]
    public async Task CreateAsync_ShouldRestorePersistedRaftLog()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            string statePath = Path.Combine(directory, "raft-state.json");
            string logPath = Path.Combine(directory, "raft-log.json");

            var stateStore = new RaftStateStore(statePath);
            var logStore = new RaftLogStore(logPath);

            var entries = new[]
            {
                new RaftLogEntry(
                    1, 1,
                    new WalRecord(
                        WalOperation.Put,
                        "user:1",
                        "Alice"u8.ToArray())),

                new RaftLogEntry(
                    2, 2,
                    new WalRecord(
                        WalOperation.Put,
                        "user:2",
                        "Bob"u8.ToArray()))
            };

            await stateStore.SaveAsync(
                new RaftPersistentState(2, "node-1"));

            await logStore.SaveAsync(entries);

            // Simulate restarting the process.
            var restarted = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(statePath),
                new RaftLogStore(logPath));

            Assert.Equal(2, restarted.CurrentTerm);
            Assert.Equal("node-1", restarted.VotedFor);
            Assert.Equal(RaftNodeState.Follower, restarted.State);

            Assert.Equal(2, restarted.LastLogIndex);
            Assert.Equal(2, restarted.LastLogTerm);

            Assert.Equal(
                "Alice",
                System.Text.Encoding.UTF8.GetString(
                    restarted.Log.GetEntry(1)!.Command.Value!));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
