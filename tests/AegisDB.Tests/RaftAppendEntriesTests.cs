
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Messages;
using AegisDB.Core.Consensus.Persistence;

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
                new RaftStateStore(path));

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
                new RaftStateStore(path));

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
                new RaftStateStore(path));

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

}
