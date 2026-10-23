
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Consensus.Transport;
using AegisDB.Core.Persistence;

namespace AegisDB.Tests;

public sealed class RaftLeaderCommitTests
{
    [Fact]
    public async Task Leader_ShouldCommitAfterMajorityReplication()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-leader-commit",
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

            transport.Register(leader);
            transport.Register(follower);

            // Node 3 is unavailable, but two nodes still form a quorum.
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

            // No follower has confirmed the log yet.
            bool committed = await manager.TryAdvanceCommitAsync();

            Assert.False(committed);
            Assert.Equal(0, leader.CommitIndex);

            // An empty follower requires multiple backtracking attempts.
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

            var progress = manager.GetFollowerProgress("node-2");

            Assert.Equal(3, progress.MatchIndex);
            Assert.Equal(4, progress.NextIndex);

            // Leader + node-2 = 2 out of 3 members.
            committed = await manager.TryAdvanceCommitAsync();

            Assert.True(committed);

            var leaderState =
                await leader.GetStateSnapshotAsync();

            Assert.Equal(3, leaderState.CommitIndex);

            // A repeated calculation must not advance commitment again.
            committed = await manager.TryAdvanceCommitAsync();

            Assert.False(committed);
            Assert.Equal(3, leader.CommitIndex);

            // The follower receives the leader's new commit index
            // on a subsequent AppendEntries request.
            var heartbeat = await follower.HandleAppendEntriesAsync(
                new AegisDB.Core.Consensus.Messages.AppendEntriesRequest(
                    Term: 1,
                    LeaderId: "node-1",
                    PrevLogIndex: 3,
                    PrevLogTerm: 1,
                    Entries: [],
                    LeaderCommit: leader.CommitIndex));

            Assert.True(heartbeat.Success);
            Assert.Equal(3, follower.CommitIndex);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task Leader_ShouldNotDirectlyCommitPreviousTermEntries()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-leader-commit",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var logStore = new RaftLogStore(
                Path.Combine(directory, "leader-log.json"));

            // These entries belong to term 1.
            await logStore.SaveAsync(
            [
                new RaftLogEntry(
                    Index: 1,
                    Term: 1,
                    Command: new WalRecord(
                        WalOperation.Put,
                        "key-1",
                        [1])),

                new RaftLogEntry(
                    Index: 2,
                    Term: 1,
                    Command: new WalRecord(
                        WalOperation.Put,
                        "key-2",
                        [2]))
            ]);

            var leader = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "state.json")),
                logStore);

            // The node becomes leader in term 2.
            await leader.BecomeFollowerAsync(1);
            await leader.StartElectionAsync();

            await leader.ReceiveVoteAsync(
                voterId: "node-2",
                term: 2,
                voteGranted: true,
                electionTerm: 2);

            Assert.Equal(RaftNodeState.Leader, leader.State);

            // Even if a majority has these entries, they belong
            // to an older term and cannot be committed directly.
            bool committed = await leader.TryAdvanceLeaderCommitAsync(
                expectedTerm: 2,
                followerMatchIndexes: new long[] { 2, 0 });

            Assert.False(committed);
            Assert.Equal(0, leader.CommitIndex);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
