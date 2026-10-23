
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Consensus.Transport;
using AegisDB.Core.Persistence;

namespace AegisDB.Tests;

public sealed class RaftReplicationManagerTests
{
    [Fact]
    public async Task ReplicateAllOnce_ShouldEventuallySynchronizeAllFollowers()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-replication-manager",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            string[] members = ["node-1", "node-2", "node-3"];

            var cluster = new RaftClusterConfiguration(members);
            var transport = new InMemoryRaftTransport();

            var leaderLogStore = new RaftLogStore(
                Path.Combine(directory, "node-1-log.json"));

            var entries = Enumerable.Range(1, 3)
                .Select(index => new RaftLogEntry(
                    Index: index,
                    Term: 1,
                    Command: new WalRecord(
                        WalOperation.Put,
                        $"key-{index}",
                        BitConverter.GetBytes(index))))
                .ToArray();

            await leaderLogStore.SaveAsync(entries);

            var nodes = new Dictionary<string, RaftNode>();

            foreach (string nodeId in members)
            {
                var stateStore = new RaftStateStore(
                    Path.Combine(directory, $"{nodeId}-state.json"));

                var logStore = nodeId == "node-1"
                    ? leaderLogStore
                    : new RaftLogStore(
                        Path.Combine(directory, $"{nodeId}-log.json"));

                var node = await RaftNode.CreateAsync(
                    nodeId,
                    cluster,
                    stateStore,
                    logStore);

                transport.Register(node);
                nodes.Add(nodeId, node);
            }

            RaftNode leader = nodes["node-1"];

            // Elect node-1 before creating its replication manager.
            await leader.StartElectionAsync();

            await leader.ReceiveVoteAsync(
                voterId: "node-2",
                term: 1,
                voteGranted: true,
                electionTerm: 1);

            Assert.Equal(RaftNodeState.Leader, leader.State);

            var manager = new RaftReplicationManager(
                leader,
                transport,
                ["node-2", "node-3"]);

            IReadOnlyDictionary<string, bool>? results = null;
            bool allReplicated = false;

            // Both followers start empty. Backtracking from
            // nextIndex=4 requires up to four attempts.
            for (int attempt = 0; attempt < 4; attempt++)
            {
                results = await manager.ReplicateAllOnceAsync();

                allReplicated = results.Values.All(success => success);

                if (allReplicated)
                    break;
            }

            Assert.True(allReplicated);
            Assert.NotNull(results);
            Assert.True(results["node-2"]);
            Assert.True(results["node-3"]);

            foreach (string followerId in members.Skip(1))
            {
                RaftNode follower = nodes[followerId];

                var snapshot =
                    await follower.GetStateSnapshotAsync();

                Assert.Equal(3, snapshot.LastLogIndex);

                var progress =
                    manager.GetFollowerProgress(followerId);

                Assert.Equal(3, progress.MatchIndex);
                Assert.Equal(4, progress.NextIndex);

                // Verify actual replicated commands, not just counts.
                for (int index = 1; index <= 3; index++)
                {
                    RaftLogEntry? entry =
                        follower.Log.GetEntry(index);

                    Assert.NotNull(entry);
                    Assert.Equal(1, entry.Term);
                    Assert.Equal($"key-{index}", entry.Command.Key);

                    Assert.Equal(
                        BitConverter.GetBytes(index),
                        entry.Command.Value);
                }

                // Replicated entries must survive reconstruction.
                var recovered = await RaftNode.CreateAsync(
                    followerId,
                    cluster,
                    new RaftStateStore(
                        Path.Combine(directory, $"{followerId}-state.json")),
                    new RaftLogStore(
                        Path.Combine(directory, $"{followerId}-log.json")));

                Assert.Equal(3, recovered.Log.LastIndex);
            }
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
