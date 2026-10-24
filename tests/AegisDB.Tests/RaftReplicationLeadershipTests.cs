
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Consensus.Transport;

namespace AegisDB.Tests;

public sealed class RaftReplicationLeadershipTests
{
    [Fact]
    public async Task ReplicationService_ShouldStopAfterLeadershipLoss()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-leadership-test",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        using var timeout =
            new CancellationTokenSource(TimeSpan.FromSeconds(5));

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

            await leader.StartElectionAsync();

            await leader.ReceiveVoteAsync(
                voterId: "node-2",
                term: 1,
                voteGranted: true,
                electionTerm: 1);

            Assert.Equal(RaftNodeState.Leader, leader.State);

            var transport = new InMemoryRaftTransport();

            // Register followers to keep the test independent
            // of unreachable-peer transport behavior.
            var follower2 = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "follower2-state.json")),
                new RaftLogStore(
                    Path.Combine(directory, "follower2-log.json")));

            var follower3 = await RaftNode.CreateAsync(
                "node-3",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "follower3-state.json")),
                new RaftLogStore(
                    Path.Combine(directory, "follower3-log.json")));

            transport.Register(leader);
            transport.Register(follower2);
            transport.Register(follower3);

            var manager = new RaftReplicationManager(
                leader,
                transport,
                ["node-2", "node-3"]);

            var service = new RaftReplicationService(
                leader,
                manager,
                TimeSpan.FromMilliseconds(10));

            Task serviceTask = service.RunAsync(timeout.Token);

            // Simulate observing a higher term.
            await leader.BecomeFollowerAsync(2);

            // The replication service must terminate voluntarily,
            // without cancelling its token.
            await serviceTask.WaitAsync(timeout.Token);

            RaftNodeStateSnapshot snapshot =
                await leader.GetStateSnapshotAsync();

            Assert.Equal(RaftNodeState.Follower, snapshot.State);
            Assert.Equal(2, snapshot.CurrentTerm);
            Assert.True(serviceTask.IsCompletedSuccessfully);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
