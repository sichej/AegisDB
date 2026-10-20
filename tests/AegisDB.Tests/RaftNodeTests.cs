
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Messages;

namespace AegisDB.Tests;

/// <summary>
/// Tests Raft node state transitions and election behavior.
/// </summary>
public sealed class RaftNodeTests
{
    [Fact]
    public void StartElection_ShouldBecomeCandidateAndVoteForSelf()
    {
        var node = new RaftNode("node-1");

        node.StartElection();

        Assert.Equal(RaftNodeState.Candidate, node.State);
        Assert.Equal(1, node.CurrentTerm);
        Assert.Equal("node-1", node.VotedFor);
    }

    [Fact]
    public void StartElection_WhenAlreadyCandidate_ShouldStartNewTerm()
    {
        var node = new RaftNode("node-1");

        node.StartElection();
        node.StartElection();

        Assert.Equal(RaftNodeState.Candidate, node.State);
        Assert.Equal(2, node.CurrentTerm);
        Assert.Equal("node-1", node.VotedFor);
    }


    [Fact]
    public void BecomeFollower_WithHigherTerm_ShouldResetVote()
    {
        var node = new RaftNode("node-1");

        node.StartElection();

        node.BecomeFollower(5);

        Assert.Equal(RaftNodeState.Follower, node.State);
        Assert.Equal(5, node.CurrentTerm);
        Assert.Null(node.VotedFor);
    }

    [Fact]
    public void BecomeFollower_WithSameTerm_ShouldPreserveVote()
    {
        var node = new RaftNode("node-1");

        node.StartElection();

        node.BecomeFollower(1);

        Assert.Equal(RaftNodeState.Follower, node.State);
        Assert.Equal(1, node.CurrentTerm);
        Assert.Equal("node-1", node.VotedFor);
    }

    [Fact]
    public void BecomeFollower_WithOlderTerm_ShouldThrow()
    {
        var node = new RaftNode("node-1");

        node.StartElection();

        Assert.Throws<ArgumentOutOfRangeException>(
            () => node.BecomeFollower(0)
        );
    }

    [Fact]
    public void HandleRequestVote_ShouldGrantVoteToEligibleCandidate()
    {
        var node = new RaftNode("node-1");

        var request = new RequestVoteRequest(
            Term: 1,
            CandidateId: "node-2",
            LastLogIndex: 0,
            LastLogTerm: 0
        );

        var response = node.HandleRequestVote(request);

        Assert.True(response.VoteGranted);
        Assert.Equal(1, response.Term);
        Assert.Equal("node-2", node.VotedFor);
        Assert.Equal(RaftNodeState.Follower, node.State);
    }

    [Fact]
    public void HandleRequestVote_ShouldRejectOutdatedLog()
    {
        var node = new RaftNode("node-1");

        node.UpdateLastLogInfo(index: 10, term: 2);

        var request = new RequestVoteRequest(
            Term: 3,
            CandidateId: "node-2",
            LastLogIndex: 8,
            LastLogTerm: 2
        );

        var response = node.HandleRequestVote(request);

        Assert.False(response.VoteGranted);
        Assert.Equal(3, node.CurrentTerm);
        Assert.Null(node.VotedFor);
    }


    [Fact]
    public void ReceiveVote_WithMajority_ShouldBecomeLeader()
    {
        var node = new RaftNode("node-1");

        node.StartElection();

        // The candidate already has its own vote.
        Assert.Equal(1, node.VotesReceived);

        bool elected = node.ReceiveVote(
            voterId: "node-2",
            term: 1,
            voteGranted: true,
            clusterSize: 3
        );

        Assert.True(elected);
        Assert.Equal(RaftNodeState.Leader, node.State);
        Assert.Equal(2, node.VotesReceived);
    }

    [Fact]
    public void ReceiveVote_DuplicateVote_ShouldNotCountTwice()
    {
        var node = new RaftNode("node-1");

        node.StartElection();

        node.ReceiveVote("node-2", 1, true, 5);
        node.ReceiveVote("node-2", 1, true, 5);

        Assert.Equal(2, node.VotesReceived);
        Assert.Equal(RaftNodeState.Candidate, node.State);
    }

    [Fact]
    public void ReceiveVote_HigherTerm_ShouldBecomeFollower()
    {
        var node = new RaftNode("node-1");

        node.StartElection();

        bool elected = node.ReceiveVote(
            voterId: "node-2",
            term: 2,
            voteGranted: false,
            clusterSize: 3
        );

        Assert.False(elected);
        Assert.Equal(RaftNodeState.Follower, node.State);
        Assert.Equal(2, node.CurrentTerm);
        Assert.Equal(0, node.VotesReceived);
    }


}
