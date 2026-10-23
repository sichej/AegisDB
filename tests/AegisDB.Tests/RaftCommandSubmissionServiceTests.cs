
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Consensus.Transport;
using AegisDB.Core.Persistence;

namespace AegisDB.Tests;

public sealed class RaftCommandSubmissionServiceTests
{
    [Fact]
    public async Task SubmitAsync_ShouldCommitAndApplyAfterMajorityReplication()
    {
        string directory = CreateTestDirectory();

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var transport = new InMemoryRaftTransport();

            var leader = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "leader-state.json")),
                new RaftLogStore(
                    Path.Combine(directory, "leader-log.json")));

            var follower = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "follower-state.json")),
                new RaftLogStore(
                    Path.Combine(directory, "follower-log.json")));

            transport.Register(leader);
            transport.Register(follower);

            // Node 3 is unavailable. Leader + node 2 form a quorum.
            await ElectLeaderAsync(leader);

            var replicationManager = new RaftReplicationManager(
                leader,
                transport,
                ["node-2", "node-3"]);

            var submissionService = new RaftCommandSubmissionService(
                leader,
                replicationManager,
                TimeSpan.FromMilliseconds(10));

            using var timeout = new CancellationTokenSource(
                TimeSpan.FromSeconds(5));

            long index = await submissionService.SubmitAsync(
                new WalRecord(
                    WalOperation.Put,
                    "user:1",
                    [65, 108, 105, 99, 101]),
                timeout.Token);

            Assert.Equal(1, index);

            RaftNodeStateSnapshot snapshot =
                await leader.GetStateSnapshotAsync();

            Assert.Equal(1, snapshot.LastLogIndex);
            Assert.Equal(1, snapshot.CommitIndex);
            Assert.Equal(1, snapshot.LastApplied);
            Assert.Equal(RaftNodeState.Leader, snapshot.State);

            // The follower must have persisted the command.
            RaftLogEntry? replicatedEntry =
                follower.Log.GetEntry(index);

            Assert.NotNull(replicatedEntry);
            Assert.Equal("user:1", replicatedEntry.Command.Key);
            Assert.Equal(
                new byte[] { 65, 108, 105, 99, 101 },
                replicatedEntry.Command.Value);

            var progress =
                replicationManager.GetFollowerProgress("node-2");

            Assert.Equal(1, progress.MatchIndex);
            Assert.Equal(2, progress.NextIndex);

            // The command must survive reconstruction.
            var recoveredFollower = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "follower-state.json")),
                new RaftLogStore(
                    Path.Combine(directory, "follower-log.json")));

            Assert.Equal(
                "user:1",
                recoveredFollower.Log.GetEntry(1)!.Command.Key);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task SubmitAsync_WithoutQuorum_ShouldNotCommitOrApply()
    {
        string directory = CreateTestDirectory();

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

            await ElectLeaderAsync(leader);

            // Neither follower is registered in the transport.
            var transport = new InMemoryRaftTransport();

            var manager = new RaftReplicationManager(
                leader,
                transport,
                ["node-2", "node-3"]);

            var service = new RaftCommandSubmissionService(
                leader,
                manager,
                TimeSpan.FromMilliseconds(10));

            using var timeout = new CancellationTokenSource(
                TimeSpan.FromMilliseconds(300));

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => service.SubmitAsync(
                    new WalRecord(
                        WalOperation.Put,
                        "user:1",
                        [1, 2, 3]),
                    timeout.Token));

            RaftNodeStateSnapshot snapshot =
                await leader.GetStateSnapshotAsync();

            // A local append is allowed without a quorum.
            Assert.Equal(1, snapshot.LastLogIndex);

            // But it must not become committed or applied.
            Assert.Equal(0, snapshot.CommitIndex);
            Assert.Equal(0, snapshot.LastApplied);

            // Cancellation must not erase the uncommitted entry.
            Assert.NotNull(leader.Log.GetEntry(1));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static async Task ElectLeaderAsync(RaftNode node)
    {
        await node.StartElectionAsync();

        await node.ReceiveVoteAsync(
            voterId: "node-2",
            term: node.CurrentTerm,
            voteGranted: true,
            electionTerm: node.CurrentTerm);

        Assert.Equal(RaftNodeState.Leader, node.State);
    }

    private static string CreateTestDirectory()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-command-submission",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        return directory;
    }
}
