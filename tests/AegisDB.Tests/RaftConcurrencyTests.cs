
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Messages;
using AegisDB.Core.Consensus.Persistence;

namespace AegisDB.Tests;

public sealed class RaftConcurrencyTests
{
    [Fact]
    public async Task ConcurrentVoteRequests_ShouldGrantAtMostOneVote()
    {
        string directory = CreateTestDirectory();

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var node = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "state.json")));

            var request2 = new RequestVoteRequest(
                Term: 5,
                CandidateId: "node-2",
                LastLogIndex: 0,
                LastLogTerm: 0);

            var request3 = new RequestVoteRequest(
                Term: 5,
                CandidateId: "node-3",
                LastLogIndex: 0,
                LastLogTerm: 0);

            // Both requests attempt to acquire the same state lock.
            Task<RequestVoteResponse> vote2 =
                node.HandleRequestVoteAsync(request2);

            Task<RequestVoteResponse> vote3 =
                node.HandleRequestVoteAsync(request3);

            RequestVoteResponse[] responses =
                await Task.WhenAll(vote2, vote3);

            Assert.Single(responses, r => r.VoteGranted);

            RaftNodeStateSnapshot snapshot =
                await node.GetStateSnapshotAsync();

            Assert.Equal(5, snapshot.CurrentTerm);
            Assert.Contains(
                snapshot.VotedFor,
                new[] { "node-2", "node-3" });
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static string CreateTestDirectory()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-concurrency",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);
        return directory;
    }



    [Fact]
    public async Task ConcurrentHigherTerms_ShouldNeverDecreaseCurrentTerm()
    {
        string directory = CreateTestDirectory();

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var node = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "state.json")));

            long[] terms = [10, 7, 12, 9];

            async Task AttemptTransitionAsync(long term)
            {
                try
                {
                    await node.BecomeFollowerAsync(term);
                }
                catch (ArgumentOutOfRangeException)
                {
                    // An older term may be rejected if another
                    // transition has already advanced the node.
                }
            }

            await Task.WhenAll(
                terms.Select(AttemptTransitionAsync));

            RaftNodeStateSnapshot snapshot =
                await node.GetStateSnapshotAsync();

            Assert.Equal(12, snapshot.CurrentTerm);
            Assert.Equal(RaftNodeState.Follower, snapshot.State);
            Assert.Null(snapshot.VotedFor);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }


    [Fact]
    public async Task StaleHeartbeatResponse_ShouldNotDemoteNewerLeader()
    {
        string directory = CreateTestDirectory();

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var node = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "state.json")));

            // Elect node-1 in term 1.
            await node.StartElectionAsync();

            await node.ReceiveVoteAsync(
                voterId: "node-2",
                term: 1,
                voteGranted: true,
                electionTerm: 1);

            Assert.Equal(RaftNodeState.Leader, node.State);

            // Move to term 2 and elect the same node again.
            await node.BecomeFollowerAsync(2);
            await node.StartElectionAsync();

            await node.ReceiveVoteAsync(
                voterId: "node-2",
                term: 3,
                voteGranted: true,
                electionTerm: 3);

            Assert.Equal(RaftNodeState.Leader, node.State);

            // A delayed response from term 1 carries term 3.
            // It must not demote the current term-3 leader.
            bool steppedDown =
                await node.StepDownIfHigherTermAsync(3);

            RaftNodeStateSnapshot snapshot =
                await node.GetStateSnapshotAsync();

            Assert.False(steppedDown);
            Assert.Equal(RaftNodeState.Leader, snapshot.State);
            Assert.Equal(3, snapshot.CurrentTerm);

            // Only a strictly higher term requires stepping down.
            steppedDown = await node.StepDownIfHigherTermAsync(4);

            snapshot = await node.GetStateSnapshotAsync();

            Assert.True(steppedDown);
            Assert.Equal(RaftNodeState.Follower, snapshot.State);
            Assert.Equal(4, snapshot.CurrentTerm);
            Assert.Null(snapshot.VotedFor);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }


}
