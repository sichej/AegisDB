
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Consensus.Transport;
using AegisDB.Core.Persistence;

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


            // STEP 1: Identify the surviving follower.
            RaftNode survivingFollower = survivors.Single(
                node => node.NodeId != newLeader.NodeId);

            // STEP 2: Append a new command after the leadership change.
            // The runtime's replication service must replicate it without
            // any explicit replication call from the test.
            byte[] value = System.Text.Encoding.UTF8.GetBytes("Alice");

            RaftLogEntry entry = await newLeader.AppendLeaderCommandAsync(
                new WalRecord(
                    WalOperation.Put,
                    "user:1",
                    value),
                testCts.Token);

            // STEP 3: Wait for the new leader to replicate and commit
            // the command through its background replication service.
            await WaitUntilAsync(
                () => newLeader.CommitIndex >= entry.Index &&
                      newLeader.LastApplied >= entry.Index,
                testCts.Token);

            Assert.Equal(entry.Index, newLeader.CommitIndex);

            RaftLogEntry? leaderEntry = newLeader.Log.GetEntry(entry.Index);

            Assert.NotNull(leaderEntry);
            Assert.Equal("user:1", leaderEntry.Command.Key);
            Assert.Equal(value, leaderEntry.Command.Value);

            // STEP 4: Verify that the surviving follower receives
            // both the entry and the updated commit index.
            await WaitUntilAsync(
                () => survivingFollower.Log.LastIndex >= entry.Index &&
                      survivingFollower.CommitIndex >= entry.Index,
                testCts.Token);

            RaftLogEntry? followerEntry =
                survivingFollower.Log.GetEntry(entry.Index);

            Assert.NotNull(followerEntry);
            Assert.Equal(entry.Term, followerEntry.Term);
            Assert.Equal(value, followerEntry.Command.Value);

            // STEP 5: Restart the failed node from its persisted files.
            // The old runtime remains stopped; the recovered node starts
            // in the Follower state.
            RaftNode recoveredNode = await RaftNode.CreateAsync(
                oldLeaderId,
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, $"{oldLeaderId}-state.json")),
                new RaftLogStore(
                    Path.Combine(directory, $"{oldLeaderId}-log.json")),
                cancellationToken: testCts.Token);

            Assert.Equal(RaftNodeState.Follower, recoveredNode.State);

            // Make the recovered node reachable to the active leader.
            transport.Register(recoveredNode);

            // STEP 6: The active leader must automatically detect that
            // this follower is behind and replicate the missing entry.
            await WaitUntilAsync(
                () => recoveredNode.Log.LastIndex >= entry.Index &&
                      recoveredNode.CommitIndex >= entry.Index,
                testCts.Token);

            RaftLogEntry? recoveredEntry =
                recoveredNode.Log.GetEntry(entry.Index);

            Assert.NotNull(recoveredEntry);
            Assert.Equal(entry.Index, recoveredEntry.Index);
            Assert.Equal(entry.Term, recoveredEntry.Term);
            Assert.Equal("user:1", recoveredEntry.Command.Key);
            Assert.Equal(value, recoveredEntry.Command.Value);

            // STEP 7: Verify persistence after catch-up.
            RaftNode persistedRecovery = await RaftNode.CreateAsync(
                oldLeaderId,
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, $"{oldLeaderId}-state.json")),
                new RaftLogStore(
                    Path.Combine(directory, $"{oldLeaderId}-log.json")),
                cancellationToken: testCts.Token);

            RaftLogEntry? persistedEntry =
                persistedRecovery.Log.GetEntry(entry.Index);

            Assert.NotNull(persistedEntry);
            Assert.Equal(value, persistedEntry.Command.Value);

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
