
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Messages;
using AegisDB.Core.Consensus.Persistence;

namespace AegisDB.Tests;

public sealed class RaftHeartbeatTimerTests
{
    [Fact]
    public async Task ValidHeartbeat_ShouldResetElectionTimer()
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

            var timer = new RaftElectionTimer(
                TimeSpan.FromSeconds(5),
                TimeSpan.FromSeconds(6));

            var node = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "raft-state.json")),
                electionTimer: timer);

            Task<bool> pendingTimeout = timer.WaitForTimeoutAsync();

            var response = await node.HandleAppendEntriesAsync(
                new AppendEntriesRequest(
                    Term: 1,
                    LeaderId: "node-1",
                    PrevLogIndex: 0,
                    PrevLogTerm: 0,
                    Entries: [],
                    LeaderCommit: 0));

            Assert.True(response.Success);

            // A valid heartbeat cancels the pending timeout.
            Assert.False(await pendingTimeout);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }


    [Fact]
    public async Task TryStartElection_ShouldRejectStaleTimerGeneration()
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

            var timer = new RaftElectionTimer(
                TimeSpan.FromSeconds(5),
                TimeSpan.FromSeconds(6));

            var node = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "raft-state.json")),
                electionTimer: timer);

            long oldGeneration = timer.Generation;

            await node.HandleAppendEntriesAsync(
                new AppendEntriesRequest(
                    Term: 1,
                    LeaderId: "node-1",
                    PrevLogIndex: 0,
                    PrevLogTerm: 0,
                    Entries: [],
                    LeaderCommit: 0));

            bool started = await node.TryStartElectionAsync(oldGeneration);

            Assert.False(started);
            Assert.Equal(RaftNodeState.Follower, node.State);
            Assert.Equal(1, node.CurrentTerm);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

}
