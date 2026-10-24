
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Consensus.Transport;
using AegisDB.Core.Persistence;

namespace AegisDB.Tests;

public sealed class RaftReplicationServiceTests
{
    [Fact]
    public async Task ReconnectedFollower_ShouldCatchUpWithoutNewClientCommands()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-replication-service",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        using var cts = new CancellationTokenSource(
            TimeSpan.FromSeconds(10));

        Task? serviceTask = null;

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var transport = new InMemoryRaftTransport();

            var leaderLogStore = new RaftLogStore(
                Path.Combine(directory, "leader-log.json"));

            // The leader already has a persistent, uncommitted entry.
            await leaderLogStore.SaveAsync(
            [
                new RaftLogEntry(
                    Index: 1,
                    Term: 1,
                    Command: new WalRecord(
                        WalOperation.Put,
                        "user:1",
                        [65, 108, 105, 99, 101]))
            ]);

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
                new RaftLogStore(
                    Path.Combine(directory, "follower-log.json")));

            transport.Register(leader);

            // Elect the leader using the vote-processing API.
            await leader.StartElectionAsync();

            await leader.ReceiveVoteAsync(
                voterId: "node-2",
                term: 1,
                voteGranted: true,
                electionTerm: 1);

            Assert.Equal(RaftNodeState.Leader, leader.State);

            // The follower is not registered yet, so replication
            // attempts cannot reach a quorum.
            var manager = new RaftReplicationManager(
                leader,
                transport,
                ["node-2", "node-3"]);

            var service = new RaftReplicationService(
                leader,
                manager,
                TimeSpan.FromMilliseconds(20));

            serviceTask = service.RunAsync(cts.Token);

            // Allow at least one failed replication round.
            // This delay is not used to prove synchronization.
            await Task.Delay(100, cts.Token);

            var before = await leader.GetStateSnapshotAsync(
                cts.Token);

            Assert.Equal(0, before.CommitIndex);
            Assert.Equal(0, follower.Log.LastIndex);

            // Simulate the follower becoming reachable again.
            transport.Register(follower);

            // No new client commands are submitted here.
            // The background service must synchronize the follower.
            while (true)
            {
                cts.Token.ThrowIfCancellationRequested();

                var leaderState =
                    await leader.GetStateSnapshotAsync(cts.Token);

                var followerState =
                    await follower.GetStateSnapshotAsync(cts.Token);

                if (leaderState.CommitIndex == 1 &&
                    followerState.CommitIndex == 1)
                {
                    break;
                }

                await Task.Delay(10, cts.Token);
            }

            var finalLeader =
                await leader.GetStateSnapshotAsync(cts.Token);

            var finalFollower =
                await follower.GetStateSnapshotAsync(cts.Token);

            Assert.Equal(1, finalLeader.CommitIndex);
            Assert.Equal(1, finalLeader.LastApplied);

            Assert.Equal(1, finalFollower.LastLogIndex);
            Assert.Equal(1, finalFollower.CommitIndex);

            var progress = manager.GetFollowerProgress("node-2");

            Assert.Equal(1, progress.MatchIndex);
            Assert.Equal(2, progress.NextIndex);

            RaftLogEntry? replicated =
                follower.Log.GetEntry(1);

            Assert.NotNull(replicated);
            Assert.Equal("user:1", replicated.Command.Key);
            Assert.Equal(
                new byte[] { 65, 108, 105, 99, 101 },
                replicated.Command.Value);

            // Verify that replication persisted the entry.
            var recovered = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "follower-state.json")),
                new RaftLogStore(
                    Path.Combine(directory, "follower-log.json")));

            Assert.Equal(1, recovered.Log.LastIndex);
            Assert.Equal(
                "user:1",
                recovered.Log.GetEntry(1)!.Command.Key);
        }
        finally
        {
            cts.Cancel();

            try
            {
                if (serviceTask is not null)
                    await serviceTask;
            }
            catch (OperationCanceledException)
                when (cts.IsCancellationRequested)
            {
                // Expected during shutdown.
            }
            finally
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }
}
