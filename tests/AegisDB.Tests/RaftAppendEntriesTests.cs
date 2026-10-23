
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Messages;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Persistence;

namespace AegisDB.Tests;

public sealed class RaftAppendEntriesTests
{
    [Fact]
    public async Task Heartbeat_WithValidTerm_ShouldBecomeFollower()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            string path = Path.Combine(directory, "raft-state.json");

            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var node = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "raft-state.json")),
                new RaftLogStore(
                    Path.Combine(directory, "raft-log.json")));

            await node.StartElectionAsync();

            var response = await node.HandleAppendEntriesAsync(
                new AppendEntriesRequest(
                    Term: 2,
                    LeaderId: "node-1",
                    PrevLogIndex: 0,
                    PrevLogTerm: 0,
                    Entries: [],
                    LeaderCommit: 0));

            Assert.True(response.Success);
            Assert.Equal(2, node.CurrentTerm);
            Assert.Null(node.VotedFor);
            Assert.Equal(RaftNodeState.Follower, node.State);

            // The newer term must survive a restart.
            var restarted = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(path));

            Assert.Equal(2, restarted.CurrentTerm);
            Assert.Null(restarted.VotedFor);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task Heartbeat_WithOlderTerm_ShouldBeRejected()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            string path = Path.Combine(directory, "raft-state.json");

            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var node = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "raft-state.json")),
                new RaftLogStore(
                    Path.Combine(directory, "raft-log.json")));

            await node.BecomeFollowerAsync(5);

            var response = await node.HandleAppendEntriesAsync(
                new AppendEntriesRequest(
                    Term: 3,
                    LeaderId: "node-1",
                    PrevLogIndex: 0,
                    PrevLogTerm: 0,
                    Entries: [],
                    LeaderCommit: 0));

            Assert.False(response.Success);
            Assert.Equal(5, response.Term);
            Assert.Equal(5, node.CurrentTerm);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }


    [Fact]
    public async Task Heartbeat_WithMismatchedPreviousLog_ShouldFail()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            string path = Path.Combine(directory, "raft-state.json");

            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var node = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "raft-state.json")),
                new RaftLogStore(
                    Path.Combine(directory, "raft-log.json")));

            var response = await node.HandleAppendEntriesAsync(
                new AppendEntriesRequest(
                    Term: 3,
                    LeaderId: "node-1",
                    PrevLogIndex: 5,
                    PrevLogTerm: 2,
                    Entries: [],
                    LeaderCommit: 0));

            Assert.False(response.Success);

            // Even if the log does not match, the higher term
            // must still be recorded.
            Assert.Equal(3, node.CurrentTerm);
            Assert.Equal(RaftNodeState.Follower, node.State);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }


    [Fact]
    public async Task AppendEntries_ShouldReplaceUncommittedConflicts()
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

            static RaftLogEntry Entry(long index, long term) =>
                new(index, term,
                    new WalRecord(
                        WalOperation.Put,
                        $"key-{index}",
                        [1, 2, 3]));

            var logStore = new RaftLogStore(
                Path.Combine(directory, "raft-log.json"));

            await logStore.SaveAsync(
                [Entry(1, 1), Entry(2, 1), Entry(3, 2), Entry(4, 2), Entry(5, 2)]);

            var node = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "raft-state.json")),
                logStore);

            await node.AdvanceCommitIndexAsync(3);

            var response = await node.HandleAppendEntriesAsync(
                new AppendEntriesRequest(
                    Term: 3,
                    LeaderId: "node-1",
                    PrevLogIndex: 3,
                    PrevLogTerm: 2,
                    Entries: [Entry(4, 3), Entry(5, 3)],
                    LeaderCommit: 3));

            Assert.True(response.Success);
            Assert.Equal(5, node.Log.LastIndex);
            Assert.Equal(3, node.Log.GetEntry(4)?.Term);
            Assert.Equal(3, node.Log.GetEntry(5)?.Term);
            Assert.Equal(3, node.CommitIndex);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }


    [Fact]
    public async Task AppendEntries_ShouldRejectCommittedConflict()
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

            static RaftLogEntry Entry(long index, long term) =>
                new(index, term,
                    new WalRecord(
                        WalOperation.Put,
                        $"key-{index}",
                        [1, 2, 3]));

            var logStore = new RaftLogStore(
                Path.Combine(directory, "raft-log.json"));

            await logStore.SaveAsync(
                [Entry(1, 1), Entry(2, 1), Entry(3, 2), Entry(4, 2)]);

            var node = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "raft-state.json")),
                logStore);

            await node.AdvanceCommitIndexAsync(4);

            var response = await node.HandleAppendEntriesAsync(
                new AppendEntriesRequest(
                    Term: 3,
                    LeaderId: "node-1",
                    PrevLogIndex: 3,
                    PrevLogTerm: 2,
                    Entries: [Entry(4, 3)],
                    LeaderCommit: 4));

            Assert.False(response.Success);
            Assert.Equal(2, node.Log.GetEntry(4)?.Term);
            Assert.Equal(4, node.CommitIndex);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }


    [Fact]
    public async Task AppendEntries_ShouldPersistReplicatedEntriesBeforeSuccess()
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

            var node = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(statePath),
                new RaftLogStore(logPath));

            var entry = new RaftLogEntry(
                Index: 1,
                Term: 1,
                Command: new WalRecord(
                    WalOperation.Put,
                    "user:1",
                    "Alice"u8.ToArray()));

            var response = await node.HandleAppendEntriesAsync(
                new AppendEntriesRequest(
                    Term: 1,
                    LeaderId: "node-1",
                    PrevLogIndex: 0,
                    PrevLogTerm: 0,
                    Entries: [entry],
                    LeaderCommit: 0));

            Assert.True(response.Success);

            // Recreate the node from its persisted state.
            var restarted = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(statePath),
                new RaftLogStore(logPath));

            Assert.Equal(1, restarted.LastLogIndex);
            Assert.Equal(1, restarted.LastLogTerm);

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


    [Fact]
    public async Task AppendEntries_ShouldAdvanceCommitIndex()
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

            var node = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "raft-state.json")),
                new RaftLogStore(
                    Path.Combine(directory, "raft-log.json")));

            var entries = Enumerable.Range(1, 3)
                .Select(index => new RaftLogEntry(
                    Index: index,
                    Term: 1,
                    Command: new WalRecord(
                        WalOperation.Put,
                        $"key-{index}",
                        [1, 2, 3])))
                .ToArray();

            var response = await node.HandleAppendEntriesAsync(
                new AppendEntriesRequest(
                    Term: 1,
                    LeaderId: "node-1",
                    PrevLogIndex: 0,
                    PrevLogTerm: 0,
                    Entries: entries,
                    LeaderCommit: 2));

            Assert.True(response.Success);
            Assert.Equal(3, node.LastLogIndex);
            Assert.Equal(2, node.CommitIndex);

            // Committing does not automatically apply commands yet.
            Assert.Equal(0, node.LastApplied);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }



    [Fact]
    public async Task AppendEntries_Heartbeat_ShouldNotCommitUnverifiedSuffix()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-commit-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var stateStore = new RaftStateStore(
                Path.Combine(directory, "state.json"));

            var logStore = new RaftLogStore(
                Path.Combine(directory, "log.json"));

            // Seed ten log entries. The first three belong to term 4,
            // while the remaining entries may be divergent.
            var entries = Enumerable.Range(1, 10)
                .Select(index => new RaftLogEntry(
                    Index: index,
                    Term: index <= 3 ? 4 : 5,
                    Command: new WalRecord(
                        WalOperation.Put,
                        $"key-{index}",
                        BitConverter.GetBytes(index))))
                .ToArray();

            await logStore.SaveAsync(entries);

            var follower = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                stateStore,
                logStore);

            var initial = await follower.GetStateSnapshotAsync();

            Assert.Equal(10, initial.LastLogIndex);
            Assert.Equal(0, initial.CommitIndex);

            // The heartbeat verifies only the prefix ending at index 3.
            // LeaderCommit=8 must NOT commit the unverified suffix.
            var response = await follower.HandleAppendEntriesAsync(
                new AppendEntriesRequest(
                    Term: 6,
                    LeaderId: "node-1",
                    PrevLogIndex: 3,
                    PrevLogTerm: 4,
                    Entries: [],
                    LeaderCommit: 8));

            Assert.True(response.Success);

            var snapshot = await follower.GetStateSnapshotAsync();

            Assert.Equal(3, snapshot.CommitIndex);
            Assert.Equal(10, snapshot.LastLogIndex);
            Assert.Equal(0, snapshot.LastApplied);
            Assert.Equal(6, snapshot.CurrentTerm);
            Assert.Equal(RaftNodeState.Follower, snapshot.State);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

}
