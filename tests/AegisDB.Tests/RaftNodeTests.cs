
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Messages;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Persistence;

namespace AegisDB.Tests;

/// <summary>
/// Tests Raft node state transitions and election behavior.
/// </summary>
public sealed class RaftNodeTests
{

    private static RaftNode CreateNode(
        IReadOnlyList<RaftLogEntry>? initialEntries = null)
    {
        var cluster = new RaftClusterConfiguration(
            ["node-1", "node-2", "node-3", "node-4", "node-5"]
        );

        return new RaftNode(
            "node-1", cluster, initialEntries: initialEntries ?? []);
    }

    [Fact]
    public void StartElection_ShouldBecomeCandidateAndVoteForSelf()
    {
        var node = CreateNode();

        node.StartElection();

        Assert.Equal(RaftNodeState.Candidate, node.State);
        Assert.Equal(1, node.CurrentTerm);
        Assert.Equal("node-1", node.VotedFor);
    }

    [Fact]
    public void StartElection_WhenAlreadyCandidate_ShouldStartNewTerm()
    {
        var node = CreateNode();

        node.StartElection();
        node.StartElection();

        Assert.Equal(RaftNodeState.Candidate, node.State);
        Assert.Equal(2, node.CurrentTerm);
        Assert.Equal("node-1", node.VotedFor);
    }


    [Fact]
    public void BecomeFollower_WithHigherTerm_ShouldResetVote()
    {
        var node = CreateNode();

        node.StartElection();

        node.BecomeFollower(5);

        Assert.Equal(RaftNodeState.Follower, node.State);
        Assert.Equal(5, node.CurrentTerm);
        Assert.Null(node.VotedFor);
    }

    [Fact]
    public void BecomeFollower_WithSameTerm_ShouldPreserveVote()
    {
        var node = CreateNode();

        node.StartElection();

        node.BecomeFollower(1);

        Assert.Equal(RaftNodeState.Follower, node.State);
        Assert.Equal(1, node.CurrentTerm);
        Assert.Equal("node-1", node.VotedFor);
    }

    [Fact]
    public void BecomeFollower_WithOlderTerm_ShouldThrow()
    {
        var node = CreateNode();

        node.StartElection();

        Assert.Throws<ArgumentOutOfRangeException>(
            () => node.BecomeFollower(0)
        );
    }

    [Fact]
    public void HandleRequestVote_ShouldGrantVoteToEligibleCandidate()
    {
        var node = CreateNode();

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
        var entries = new List<RaftLogEntry>();

        // Build the actual Raft log rather than manually
        // setting its last index and term.
        for (long index = 1; index <= 10; index++)
        {
            entries.Add(new RaftLogEntry(
                Index: index,
                Term: 2,
                Command: new WalRecord(
                    WalOperation.Put,
                    $"key-{index}",
                    [1, 2, 3])
            ));
        }

        var node = CreateNode(initialEntries: entries);

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
        var node = CreateNode();

        node.StartElection();

        // The candidate votes for itself.
        Assert.Equal(1, node.VotesReceived);

        // First external vote: 2 out of 3 required votes.
        bool elected = node.ReceiveVote(
            voterId: "node-2",
            term: 1,
            voteGranted: true
        );

        Assert.False(elected);
        Assert.Equal(RaftNodeState.Candidate, node.State);

        // Second external vote: quorum reached (3 out of 5).
        elected = node.ReceiveVote(
            voterId: "node-3",
            term: 1,
            voteGranted: true
        );

        Assert.True(elected);
        Assert.Equal(RaftNodeState.Leader, node.State);
        Assert.Equal(3, node.VotesReceived);
    }

    [Fact]
    public void ReceiveVote_DuplicateVote_ShouldNotCountTwice()
    {
        var node = CreateNode();

        node.StartElection();

        node.ReceiveVote("node-2", 1, true);
        node.ReceiveVote("node-2", 1, true);

        Assert.Equal(2, node.VotesReceived);
        Assert.Equal(RaftNodeState.Candidate, node.State);
    }

    [Fact]
    public void ReceiveVote_HigherTerm_ShouldBecomeFollower()
    {
        var node = CreateNode();

        node.StartElection();

        bool elected = node.ReceiveVote(
            voterId: "node-2",
            term: 2,
            voteGranted: false
        );

        Assert.False(elected);
        Assert.Equal(RaftNodeState.Follower, node.State);
        Assert.Equal(2, node.CurrentTerm);
        Assert.Equal(0, node.VotesReceived);
    }

    [Fact]
    public void ReceiveVote_FromUnknownNode_ShouldNotBeCounted()
    {
        var cluster = new RaftClusterConfiguration(
            ["node-1", "node-2", "node-3"]
        );

        var node = new RaftNode("node-1", cluster);

        node.StartElection();

        bool elected = node.ReceiveVote(
            voterId: "unknown-node",
            term: 1,
            voteGranted: true
        );

        Assert.False(elected);
        Assert.Equal(1, node.VotesReceived);
        Assert.Equal(RaftNodeState.Candidate, node.State);
    }


    [Fact]
    public async Task Restart_ShouldRestoreTermAndVote()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            string path = Path.Combine(directory, "raft-state.json");

            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var store = new RaftStateStore(path);

            var node = await RaftNode.CreateAsync(
                "node-1", cluster, store);

            await node.StartElectionAsync();

            Assert.Equal(1, node.CurrentTerm);
            Assert.Equal("node-1", node.VotedFor);
            Assert.Equal(RaftNodeState.Candidate, node.State);

            // Simulate a restart by constructing a new node
            // from the persisted state.
            var restartedNode = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(path));

            Assert.Equal(1, restartedNode.CurrentTerm);
            Assert.Equal("node-1", restartedNode.VotedFor);
            Assert.Equal(RaftNodeState.Follower, restartedNode.State);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }


    [Fact]
    public async Task BecomeFollowerAsync_WithHigherTerm_ShouldPersistState()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            string path = Path.Combine(directory, "raft-state.json");

            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var store = new RaftStateStore(path);

            var node = await RaftNode.CreateAsync(
                "node-1", cluster, store);

            await node.StartElectionAsync();

            Assert.Equal(1, node.CurrentTerm);
            Assert.Equal("node-1", node.VotedFor);
            Assert.Equal(RaftNodeState.Candidate, node.State);

            await node.BecomeFollowerAsync(5);

            Assert.Equal(5, node.CurrentTerm);
            Assert.Null(node.VotedFor);
            Assert.Equal(RaftNodeState.Follower, node.State);

            // Restart the node and verify that the newer term
            // was durably stored.
            var restartedNode = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(path));

            Assert.Equal(5, restartedNode.CurrentTerm);
            Assert.Null(restartedNode.VotedFor);
            Assert.Equal(RaftNodeState.Follower, restartedNode.State);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }


    [Fact]
    public async Task BecomeFollowerAsync_WithSameTerm_ShouldPreserveVote()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            string path = Path.Combine(directory, "raft-state.json");

            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var node = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(path));

            await node.StartElectionAsync();
            await node.BecomeFollowerAsync(1);

            Assert.Equal(RaftNodeState.Follower, node.State);
            Assert.Equal("node-1", node.VotedFor);

            var restarted = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(path));

            Assert.Equal(1, restarted.CurrentTerm);
            Assert.Equal("node-1", restarted.VotedFor);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }


    [Fact]
    public async Task ConcurrentElections_ShouldIncrementTermWithoutLostUpdates()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            string path = Path.Combine(directory, "raft-state.json");

            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var node = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(path));

            const int electionCount = 20;

            var elections = Enumerable.Range(0, electionCount)
                .Select(_ => node.StartElectionAsync());

            await Task.WhenAll(elections);

            Assert.Equal(electionCount, node.CurrentTerm);
            Assert.Equal(RaftNodeState.Candidate, node.State);
            Assert.Equal("node-1", node.VotedFor);

            var restarted = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(path));

            Assert.Equal(electionCount, restarted.CurrentTerm);
            Assert.Equal("node-1", restarted.VotedFor);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }


    [Fact]
    public async Task HandleRequestVoteAsync_ShouldPersistGrantedVote()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            string path = Path.Combine(directory, "raft-state.json");

            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var node = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(path));

            var response = await node.HandleRequestVoteAsync(
                new RequestVoteRequest(
                    Term: 3,
                    CandidateId: "node-2",
                    LastLogIndex: 0,
                    LastLogTerm: 0));

            Assert.True(response.VoteGranted);
            Assert.Equal(3, response.Term);
            Assert.Equal("node-2", node.VotedFor);

            // The vote must survive a node restart.
            var restarted = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(path));

            Assert.Equal(3, restarted.CurrentTerm);
            Assert.Equal("node-2", restarted.VotedFor);
            Assert.Equal(RaftNodeState.Follower, restarted.State);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }


    [Fact]
    public async Task HandleRequestVoteAsync_ShouldRejectSecondCandidateInSameTerm()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            string path = Path.Combine(directory, "raft-state.json");

            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var node = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(path));

            var firstVote = await node.HandleRequestVoteAsync(
                new RequestVoteRequest(3, "node-2", 0, 0));

            var secondVote = await node.HandleRequestVoteAsync(
                new RequestVoteRequest(3, "node-3", 0, 0));

            Assert.True(firstVote.VoteGranted);
            Assert.False(secondVote.VoteGranted);
            Assert.Equal("node-2", node.VotedFor);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }


    [Fact]
    public async Task HandleRequestVoteAsync_UnknownCandidate_ShouldBeRejected()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            string path = Path.Combine(directory, "raft-state.json");

            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var node = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(path));

            var response = await node.HandleRequestVoteAsync(
                new RequestVoteRequest(
                    Term: 100,
                    CandidateId: "unknown-node",
                    LastLogIndex: 0,
                    LastLogTerm: 0));

            Assert.False(response.VoteGranted);
            Assert.Equal(0, response.Term);
            Assert.Equal(0, node.CurrentTerm);
            Assert.Null(node.VotedFor);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }


    [Fact]
    public async Task HandleRequestVoteAsync_ConcurrentCandidates_ShouldGrantOnlyOneVote()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            string path = Path.Combine(directory, "raft-state.json");

            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var node = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(path));

            var request2 = new RequestVoteRequest(
                5, "node-2", 0, 0);

            var request3 = new RequestVoteRequest(
                5, "node-3", 0, 0);

            // Both candidates request a vote in the same term.
            var responses = await Task.WhenAll(
                Task.Run(() => node.HandleRequestVoteAsync(request2)),
                Task.Run(() => node.HandleRequestVoteAsync(request3)));

            Assert.Single(responses, r => r.VoteGranted);

            Assert.Equal(5, node.CurrentTerm);
            Assert.Contains(
                node.VotedFor,
                new[] { "node-2", "node-3" });

            // Verify that the chosen vote survives a restart.
            var restarted = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(path));

            Assert.Equal(5, restarted.CurrentTerm);
            Assert.Equal(node.VotedFor, restarted.VotedFor);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public void RaftNode_ShouldReadLogMetadataFromRaftLog()
    {
        var node = CreateNode();

        Assert.Equal(0, node.LastLogIndex);
        Assert.Equal(0, node.LastLogTerm);

        node = CreateNode(initialEntries:
        [
            new RaftLogEntry(
                Index: 1,
                Term: 3,
                Command: new WalRecord(
                    WalOperation.Put,
                    "user:1",
                    "Alice"u8.ToArray()))
        ]);

        Assert.Equal(1, node.LastLogIndex);
        Assert.Equal(3, node.LastLogTerm);
    }

}
