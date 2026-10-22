
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Consensus.Transport;

namespace AegisDB.Tests;

public sealed class RaftElectionCoordinatorTests
{
    [Fact]
    public async Task RunElection_ShouldElectLeaderWithMajority()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            var members = new[] { "node-1", "node-2", "node-3" };
            var cluster = new RaftClusterConfiguration(members);
            var transport = new InMemoryRaftTransport();

            var nodes = new Dictionary<string, RaftNode>();

            foreach (string nodeId in members)
            {
                var node = await RaftNode.CreateAsync(
                    nodeId,
                    cluster,
                    new RaftStateStore(Path.Combine(
                        directory, $"{nodeId}-state.json")),
                    new RaftLogStore(Path.Combine(
                        directory, $"{nodeId}-log.json")));

                transport.Register(node);
                nodes[nodeId] = node;
            }

            var coordinator = new RaftElectionCoordinator(
                nodes["node-1"],
                transport,
                ["node-2", "node-3"]);

            bool elected = await coordinator.RunElectionAsync();

            Assert.True(elected);
            Assert.Equal(
                RaftNodeState.Leader,
                nodes["node-1"].State);

            Assert.Equal(1, nodes["node-1"].CurrentTerm);
            Assert.Equal("node-1", nodes["node-2"].VotedFor);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }


    [Fact]
    public async Task RunElection_ShouldSucceedWhenOnePeerIsUnavailable()
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

            var transport = new InMemoryRaftTransport();

            var node1 = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "node-1-state.json")));

            var node2 = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "node-2-state.json")));

            transport.Register(node1);
            transport.Register(node2);

            // Node 3 is intentionally not registered.
            var coordinator = new RaftElectionCoordinator(
                node1,
                transport,
                ["node-2", "node-3"]);

            bool elected = await coordinator.RunElectionAsync();

            Assert.True(elected);
            Assert.Equal(RaftNodeState.Leader, node1.State);
            Assert.Equal(1, node1.CurrentTerm);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

}
