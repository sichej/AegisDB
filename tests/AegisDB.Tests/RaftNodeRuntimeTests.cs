
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Consensus.Transport;

namespace AegisDB.Tests;

public sealed class RaftNodeRuntimeTests
{
    [Fact]
    public async Task Cluster_ShouldElectLeaderUsingNodeRuntimes()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        using var cts = new CancellationTokenSource(
            TimeSpan.FromSeconds(10));

        var tasks = new List<Task>();

        try
        {
            string[] members = ["node-1", "node-2", "node-3"];

            var cluster = new RaftClusterConfiguration(members);
            var transport = new InMemoryRaftTransport();

            var nodes = new List<RaftNode>();
            var runtimes = new List<RaftNodeRuntime>();

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

                var peers = members
                    .Where(id => id != nodeId)
                    .ToArray();

                var coordinator = new RaftElectionCoordinator(
                    node,
                    transport,
                    peers);

                var electionService = new RaftElectionService(
                    node,
                    timer,
                    coordinator);

                var heartbeatService = new RaftHeartbeatService(
                    node,
                    transport,
                    peers,
                    TimeSpan.FromMilliseconds(100));

                runtimes.Add(new RaftNodeRuntime(
                    node,
                    electionService,
                    heartbeatService,
                    transport: transport,
                    peerIds: peers));

                nodes.Add(node);
            }

            foreach (var runtime in runtimes)
            {
                tasks.Add(runtime.RunAsync(cts.Token));
            }

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
                await Task.WhenAll(tasks);
            }
            catch (OperationCanceledException)
                when (cts.IsCancellationRequested)
            {
                // Expected during shutdown.
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

            if (nodes.Any(n => n.State == RaftNodeState.Leader))
                return;

            await Task.Delay(20, cancellationToken);
        }
    }
}
