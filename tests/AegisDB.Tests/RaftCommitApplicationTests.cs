
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Consensus.Transport;
using AegisDB.Core.Persistence;

namespace AegisDB.Tests;

public sealed class RaftCommitApplicationTests
{
    [Fact]
    public async Task CommitAndApply_ShouldApplyEntriesAfterQuorum()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-commit-application",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var transport = new InMemoryRaftTransport();

            var leaderLogStore = new RaftLogStore(
                Path.Combine(directory, "leader-log.json"));

            var followerLogStore = new RaftLogStore(
                Path.Combine(directory, "follower-log.json"));

            var entries = new[]
            {
                new RaftLogEntry(
                    Index: 1,
                    Term: 1,
                    Command: new WalRecord(
                        WalOperation.Put,
                        "user:1",
                        [65, 108, 105, 99, 101])),

                new RaftLogEntry(
                    Index: 2,
                    Term: 1,
                    Command: new WalRecord(
                        WalOperation.Put,
                        "user:2",
                        [66, 111, 98])),

                new RaftLogEntry(
                    Index: 3,
                    Term: 1,
                    Command: new WalRecord(
                        WalOperation.Delete,
                        "user:1",
                        null))
            };

            await leaderLogStore.SaveAsync(entries);

            var leader = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "leader-state.json")),
                leaderLogStore);

            var follower = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "follower-state.json")),
                followerLogStore);

            transport.Register(leader);
            transport.Register(follower);

            // Elect node-1 in term 1.
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

            // Before replication, the leader has no quorum
            // confirmation for its log entries.
            bool committed = await manager.CommitAndApplyAsync();

            Assert.False(committed);

            var initialState =
                await leader.GetStateSnapshotAsync();

            Assert.Equal(0, initialState.CommitIndex);
            Assert.Equal(0, initialState.LastApplied);

            // Replicate to node-2. Node-3 remains unavailable.
            bool replicated = false;

            for (int attempt = 0; attempt < 4; attempt++)
            {
                var results = await manager.ReplicateAllOnceAsync();

                replicated = results["node-2"];

                if (replicated)
                    break;
            }

            Assert.True(replicated);
            Assert.Equal(3, follower.Log.LastIndex);

            // Leader + node-2 form a majority of the cluster.
            committed = await manager.CommitAndApplyAsync();

            Assert.True(committed);

            var appliedState =
                await leader.GetStateSnapshotAsync();

            Assert.Equal(3, appliedState.CommitIndex);
            Assert.Equal(3, appliedState.LastApplied);

            // Calling again must not apply the same entries twice.
            committed = await manager.CommitAndApplyAsync();

            Assert.False(committed);

            var repeatedState =
                await leader.GetStateSnapshotAsync();

            Assert.Equal(3, repeatedState.CommitIndex);
            Assert.Equal(3, repeatedState.LastApplied);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
