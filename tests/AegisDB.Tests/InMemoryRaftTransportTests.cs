
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Messages;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Consensus.Transport;

namespace AegisDB.Tests;

public sealed class InMemoryRaftTransportTests
{
    [Fact]
    public async Task RequestVote_ShouldReachTargetNode()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var transport = new InMemoryRaftTransport();

            var nodes = new List<RaftNode>();

            foreach (string nodeId in new[]
                     { "node-1", "node-2", "node-3" })
            {
                var stateStore = new RaftStateStore(
                    Path.Combine(directory, $"{nodeId}-state.json"));

                var logStore = new RaftLogStore(
                    Path.Combine(directory, $"{nodeId}-log.json"));

                var node = await RaftNode.CreateAsync(
                    nodeId,
                    cluster,
                    stateStore,
                    logStore);

                transport.Register(node);
                nodes.Add(node);
            }

            // Node 1 starts an election in term 1.
            await nodes[0].StartElectionAsync();

            var request = new RequestVoteRequest(
                Term: nodes[0].CurrentTerm,
                CandidateId: nodes[0].NodeId,
                LastLogIndex: nodes[0].LastLogIndex,
                LastLogTerm: nodes[0].LastLogTerm);

            // Deliver the RPC directly to node 2.
            var response = await transport.RequestVoteAsync(
                "node-2",
                request);

            Assert.True(response.VoteGranted);
            Assert.Equal(1, response.Term);
            Assert.Equal("node-1", nodes[1].VotedFor);
            Assert.Equal(RaftNodeState.Follower, nodes[1].State);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
