
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Consensus.Transport;
using AegisDB.Core.Persistence;

namespace AegisDB.Tests;

public sealed class RaftLogReplicatorTests
{
    [Fact]
    public async Task ReplicateOnce_ShouldEventuallyCatchUpEmptyFollower()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-replication",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var leaderLogStore = new RaftLogStore(
                Path.Combine(directory, "leader-log.json"));

            var followerLogStore = new RaftLogStore(
                Path.Combine(directory, "follower-log.json"));

            // Seed three entries in the future leader's log.
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

            // Elect node-1 in term 1.
            await leader.StartElectionAsync();

            await leader.ReceiveVoteAsync(
                voterId: "node-2",
                term: 1,
                voteGranted: true,
                electionTerm: 1);

            Assert.Equal(RaftNodeState.Leader, leader.State);

            var transport = new InMemoryRaftTransport();

            transport.Register(leader);
            transport.Register(follower);

            var progress = new RaftFollowerProgress(
                followerId: "node-2",
                leaderLastLogIndex: 3);

            var replicator = new RaftLogReplicator(
                leader,
                transport,
                progress);

            bool replicated = false;

            // The follower starts empty, so the leader must
            // backtrack until it finds the matching prefix.
            for (int attempt = 0; attempt < 4; attempt++)
            {
                replicated = await replicator.ReplicateOnceAsync();

                if (replicated)
                    break;
            }

            Assert.True(replicated);

            var followerState =
                await follower.GetStateSnapshotAsync();

            Assert.Equal(3, followerState.LastLogIndex);

            var replicationProgress = progress.GetSnapshot();

            Assert.Equal(3, replicationProgress.MatchIndex);
            Assert.Equal(4, replicationProgress.NextIndex);

            // Verify the actual replicated commands.
            for (int index = 1; index <= 3; index++)
            {
                RaftLogEntry? entry = follower.Log.GetEntry(index);

                Assert.NotNull(entry);
                Assert.Equal(index, entry.Index);
                Assert.Equal(1, entry.Term);
                Assert.Equal($"key-{index}", entry.Command.Key);

                Assert.Equal(
                    BitConverter.GetBytes(index),
                    entry.Command.Value);
            }

            // Recreate the follower to verify that the
            // replicated log was persisted before acknowledgement.
            var recoveredFollower = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "follower-state.json")),
                followerLogStore);

            Assert.Equal(3, recoveredFollower.Log.LastIndex);

            for (int index = 1; index <= 3; index++)
            {
                Assert.Equal(
                    $"key-{index}",
                    recoveredFollower.Log.GetEntry(index)!.Command.Key);
            }
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }


    [Fact]
    public async Task ReplicateOnce_ShouldConfirmExistingMatchingLog()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-replication",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var entries = Enumerable.Range(1, 3)
                .Select(index => new RaftLogEntry(
                    Index: index,
                    Term: 1,
                    Command: new WalRecord(
                        WalOperation.Put,
                        $"key-{index}",
                        BitConverter.GetBytes(index))))
                .ToArray();

            var leaderLogStore = new RaftLogStore(
                Path.Combine(directory, "leader-log.json"));

            var followerLogStore = new RaftLogStore(
                Path.Combine(directory, "follower-log.json"));

            await leaderLogStore.SaveAsync(entries);
            await followerLogStore.SaveAsync(entries);

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

            await leader.StartElectionAsync();

            await leader.ReceiveVoteAsync(
                voterId: "node-2",
                term: 1,
                voteGranted: true,
                electionTerm: 1);

            var transport = new InMemoryRaftTransport();
            transport.Register(leader);
            transport.Register(follower);

            var progress = new RaftFollowerProgress(
                "node-2",
                leaderLastLogIndex: 3);

            var replicator = new RaftLogReplicator(
                leader,
                transport,
                progress);

            bool success = await replicator.ReplicateOnceAsync();

            Assert.True(success);

            var snapshot = progress.GetSnapshot();

            Assert.Equal(3, snapshot.MatchIndex);
            Assert.Equal(4, snapshot.NextIndex);
            Assert.Equal(3, follower.Log.LastIndex);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

}
