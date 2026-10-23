
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Consensus.Transport;

namespace AegisDB.Tests;

public sealed class RaftAutomaticElectionTests
{
    [Fact]
    public async Task Cluster_ShouldElectLeaderAutomatically()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        using var cts = new CancellationTokenSource(
            TimeSpan.FromSeconds(10));

        var services = new List<Task>();

        try
        {
            string[] members = ["node-1", "node-2", "node-3"];

            var cluster = new RaftClusterConfiguration(members);
            var transport = new InMemoryRaftTransport();

            var nodes = new List<RaftNode>();
            var timers = new List<RaftElectionTimer>();

            // Create three independent Raft nodes,
            // each with its own persistent state and log.
            foreach (string nodeId in members)
            {
                var timer = new RaftElectionTimer(
                    TimeSpan.FromMilliseconds(500),
                    TimeSpan.FromMilliseconds(1000));

                var node = await RaftNode.CreateAsync(
                    nodeId,
                    cluster,
                    new RaftStateStore(
                        Path.Combine(directory, $"{nodeId}-state.json")),
                    new RaftLogStore(
                        Path.Combine(directory, $"{nodeId}-log.json")),
                    electionTimer: timer);

                transport.Register(node);

                nodes.Add(node);
                timers.Add(timer);
            }

            // Start the election loop for every node.
            for (int i = 0; i < nodes.Count; i++)
            {
                RaftNode node = nodes[i];

                var peers = members
                    .Where(id => id != node.NodeId);

                var coordinator = new RaftElectionCoordinator(
                    node,
                    transport,
                    peers);

                var service = new RaftElectionService(
                    node,
                    timers[i],
                    coordinator);

                services.Add(service.RunAsync(cts.Token));
            }

            // Observe the cluster until one leader is elected.
            // This is a bounded integration-test wait, not
            // a production leader-election timeout.
            await WaitForLeaderAsync(nodes, cts.Token);

            Assert.Contains(
                nodes,
                node => node.State == RaftNodeState.Leader);
        }
        finally
        {
            cts.Cancel();

            try
            {
                await Task.WhenAll(services);
            }
            catch (OperationCanceledException)
                when (cts.IsCancellationRequested)
            {
                // Expected when stopping election services.
            }

            Directory.Delete(directory, recursive: true);
        }
    }

    private static async Task WaitForLeaderAsync(
        IReadOnlyList<RaftNode> nodes,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (nodes.Any(
                node => node.State == RaftNodeState.Leader))
            {
                return;
            }

            await Task.Delay(20, cancellationToken);
        }
    }
}
