
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Consensus.Transport;

namespace AegisDB.Tests;

public sealed class RaftLeaderFailoverTests
{
    [Fact]
    public async Task Cluster_ShouldElectNewLeaderAfterLeaderFailure()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-failover",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        using var testCts = new CancellationTokenSource(
            TimeSpan.FromSeconds(15));

        var nodeCts = new Dictionary<string, CancellationTokenSource>();
        var tasks = new Dictionary<string, Task>();

        try
        {
            string[] members = ["node-1", "node-2", "node-3"];

            var cluster = new RaftClusterConfiguration(members);
            var transport = new InMemoryRaftTransport();

            var nodes = new Dictionary<string, RaftNode>();

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
                nodes[nodeId] = node;

                string[] peers = members
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

                var runtime = new RaftNodeRuntime(
                    node,
                    electionService,
                    heartbeatService,
                    transport: transport,
                    peerIds: peers);

                // Each node has an independent shutdown token.
                var cts = CancellationTokenSource.CreateLinkedTokenSource(
                    testCts.Token);

                nodeCts[nodeId] = cts;
                tasks[nodeId] = runtime.RunAsync(cts.Token);
            }

            // Wait for the initial leader.
            RaftNode oldLeader = await WaitForLeaderAsync(
                nodes.Values.ToArray(),
                testCts.Token);

            long oldTerm = oldLeader.CurrentTerm;
            string oldLeaderId = oldLeader.NodeId;

            // Simulate a crash: stop the runtime and disconnect the node.
            transport.Unregister(oldLeaderId);
            nodeCts[oldLeaderId].Cancel();

            try
            {
                await tasks[oldLeaderId];
            }
            catch (OperationCanceledException)
                when (nodeCts[oldLeaderId].IsCancellationRequested)
            {
                // Expected shutdown.
            }

            // Only surviving nodes are eligible for the new election.
            RaftNode[] survivors = nodes.Values
                .Where(n => n.NodeId != oldLeaderId)
                .ToArray();

            await WaitUntilAsync(
                () => survivors.Any(n =>
                    n.State == RaftNodeState.Leader &&
                    n.CurrentTerm > oldTerm),
                testCts.Token);

            RaftNode newLeader = survivors.Single(n =>
                n.State == RaftNodeState.Leader &&
                n.CurrentTerm > oldTerm);

            Assert.NotEqual(oldLeaderId, newLeader.NodeId);
            Assert.True(newLeader.CurrentTerm > oldTerm);
        }
        finally
        {
            testCts.Cancel();

            foreach (var cts in nodeCts.Values)
                cts.Cancel();

            try
            {
                await Task.WhenAll(tasks.Values);
            }
            catch (OperationCanceledException)
            {
                // Expected during test cleanup.
            }
            finally
            {
                foreach (var cts in nodeCts.Values)
                    cts.Dispose();

                Directory.Delete(directory, recursive: true);
            }
        }
    }

    private static async Task<RaftNode> WaitForLeaderAsync(
        IReadOnlyList<RaftNode> nodes,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            RaftNode? leader = nodes.FirstOrDefault(
                n => n.State == RaftNodeState.Leader);

            if (leader is not null)
                return leader;

            await Task.Delay(20, cancellationToken);
        }
    }

    private static async Task WaitUntilAsync(
        Func<bool> condition,
        CancellationToken cancellationToken)
    {
        while (!condition())
        {
            await Task.Delay(20, cancellationToken);
        }
    }
}
