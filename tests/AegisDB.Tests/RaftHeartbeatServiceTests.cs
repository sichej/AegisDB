
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Consensus.Transport;

namespace AegisDB.Tests;

public sealed class RaftHeartbeatServiceTests
{
    [Fact]
    public async Task Leader_ShouldSendHeartbeatsToFollowers()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            string[] members = ["node-1", "node-2", "node-3"];

            var cluster = new RaftClusterConfiguration(members);
            var transport = new InMemoryRaftTransport();

            var timers = members.ToDictionary(
                id => id,
                _ => new RaftElectionTimer(
                    TimeSpan.FromSeconds(5),
                    TimeSpan.FromSeconds(6)));

            var nodes = new Dictionary<string, RaftNode>();

            foreach (string nodeId in members)
            {
                var node = await RaftNode.CreateAsync(
                    nodeId,
                    cluster,
                    new RaftStateStore(
                        Path.Combine(directory, $"{nodeId}-state.json")),
                    electionTimer: timers[nodeId]);

                transport.Register(node);
                nodes[nodeId] = node;
            }

            var leader = nodes["node-1"];

            await leader.StartElectionAsync();

            await leader.ReceiveVoteAsync(
                voterId: "node-2",
                term: 1,
                voteGranted: true,
                electionTerm: 1);

            Assert.Equal(RaftNodeState.Leader, leader.State);

            // Start waiting before the heartbeat is sent.
            Task<bool> followerTimeout =
                timers["node-2"].WaitForTimeoutAsync();

            var service = new RaftHeartbeatService(
                leader,
                transport,
                ["node-2", "node-3"],
                TimeSpan.FromMilliseconds(100));

            using var cts = new CancellationTokenSource();

            Task serviceTask = service.RunAsync(cts.Token);

            try
            {
                // The follower's pending election timeout
                // should be cancelled by the heartbeat.
                Assert.False(
                    await followerTimeout.WaitAsync(
                        TimeSpan.FromSeconds(2)));

                Assert.Equal(1, nodes["node-2"].CurrentTerm);
                Assert.Equal(RaftNodeState.Follower, nodes["node-2"].State);
            }
            finally
            {
                cts.Cancel();

                try
                {
                    await serviceTask;
                }
                catch (OperationCanceledException)
                    when (cts.IsCancellationRequested)
                {
                    // Expected during shutdown.
                }
            }
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
