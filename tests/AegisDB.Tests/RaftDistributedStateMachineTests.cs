
using System.Text;
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Messages;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Consensus.Transport;
using AegisDB.Core.Persistence;
using AegisDB.Tests.Helpers;

namespace AegisDB.Tests;

public sealed class RaftDistributedStateMachineTests
{
    [Fact]
    public async Task CommittedCommands_ShouldProduceSameStateOnLeaderAndFollower()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-distributed-state",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        using var cts = new CancellationTokenSource(
            TimeSpan.FromSeconds(10));

        Task? applyTask = null;

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var transport = new InMemoryRaftTransport();

            var leaderMachine = new ObservableRaftStateMachine();
            var followerMachine = new ObservableRaftStateMachine();

            var leader = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "leader-state.json")),
                new RaftLogStore(
                    Path.Combine(directory, "leader-log.json")),
                stateMachine: leaderMachine);

            var follower = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "follower-state.json")),
                new RaftLogStore(
                    Path.Combine(directory, "follower-log.json")),
                stateMachine: followerMachine);

            transport.Register(leader);
            transport.Register(follower);

            // Node 3 is unavailable. Two nodes form a quorum.
            await leader.StartElectionAsync();

            await leader.ReceiveVoteAsync(
                voterId: "node-2",
                term: leader.CurrentTerm,
                voteGranted: true,
                electionTerm: leader.CurrentTerm);

            Assert.Equal(RaftNodeState.Leader, leader.State);

            var manager = new RaftReplicationManager(
                leader,
                transport,
                ["node-2", "node-3"]);

            var submission = new RaftCommandSubmissionService(
                leader,
                manager,
                TimeSpan.FromMilliseconds(10));

            var followerApplyService =
                new RaftStateMachineApplyService(
                    follower,
                    TimeSpan.FromMilliseconds(10));

            applyTask = followerApplyService.RunAsync(cts.Token);

            // STEP A: Submit and commit PUT.
            long putIndex = await submission.SubmitAsync(
                new WalRecord(
                    WalOperation.Put,
                    "user:1",
                    Encoding.UTF8.GetBytes("Alice")),
                cts.Token);

            Assert.Equal(1, putIndex);

            Assert.Equal(
                "Alice",
                Encoding.UTF8.GetString(
                    Assert.IsType<byte[]>(
                        leaderMachine.Get("user:1"))));

            await WaitForAppliedIndexAsync(
                follower,
                putIndex,
                cts.Token);

            Assert.Equal(
                "Alice",
                Encoding.UTF8.GetString(
                    Assert.IsType<byte[]>(
                        followerMachine.Get("user:1"))));

            // STEP B: Submit and commit DELETE.
            long deleteIndex = await submission.SubmitAsync(
                new WalRecord(
                    WalOperation.Delete,
                    "user:1",
                    null),
                cts.Token);

            Assert.Equal(2, deleteIndex);


            await WaitForAppliedIndexAsync(
                follower,
                deleteIndex,
                cts.Token);

            // Both state machines must now have the same result.
            Assert.Null(leaderMachine.Get("user:1"));
            Assert.Null(followerMachine.Get("user:1"));

            // Every committed command must be applied once,
            // and in the same order on both nodes.
            Assert.Equal(
                new long[] { 1, 2 },
                leaderMachine.GetAppliedIndexes());

            Assert.Equal(
                new long[] { 1, 2 },
                followerMachine.GetAppliedIndexes());

            var leaderState = await leader.GetStateSnapshotAsync();
            var followerState = await follower.GetStateSnapshotAsync();

            Assert.Equal(2, leaderState.CommitIndex);
            Assert.Equal(2, leaderState.LastApplied);

            Assert.Equal(2, followerState.CommitIndex);
            Assert.Equal(2, followerState.LastApplied);
        }
        finally
        {
            cts.Cancel();

            try
            {
                if (applyTask is not null)
                    await applyTask;
            }
            catch (OperationCanceledException)
                when (cts.IsCancellationRequested)
            {
                // Expected during service shutdown.
            }
            finally
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    private static async Task PropagateCommitAsync(
        RaftNode leader,
        RaftNode follower,
        CancellationToken cancellationToken)
    {
        RaftNodeStateSnapshot leaderState =
            await leader.GetStateSnapshotAsync(cancellationToken);

        var response = await follower.HandleAppendEntriesAsync(
            new AppendEntriesRequest(
                Term: leaderState.CurrentTerm,
                LeaderId: leaderState.NodeId,
                PrevLogIndex: leaderState.LastLogIndex,
                PrevLogTerm: leaderState.LastLogTerm,
                Entries: [],
                LeaderCommit: leaderState.CommitIndex),
            cancellationToken);

        Assert.True(response.Success);
    }

    private static async Task WaitForAppliedIndexAsync(
        RaftNode node,
        long expectedIndex,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var snapshot =
                await node.GetStateSnapshotAsync(cancellationToken);

            if (snapshot.LastApplied >= expectedIndex)
                return;

            await Task.Delay(10, cancellationToken);
        }
    }
}
