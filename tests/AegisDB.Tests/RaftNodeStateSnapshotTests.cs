
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;

namespace AegisDB.Tests;

public sealed class RaftNodeStateSnapshotTests
{
    [Fact]
    public async Task Snapshot_ShouldReflectCurrentConsensusState()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-snapshot-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var node = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "raft-state.json")));

            RaftNodeStateSnapshot initial =
                await node.GetStateSnapshotAsync();

            Assert.Equal(RaftNodeState.Follower, initial.State);
            Assert.Equal(0, initial.CurrentTerm);
            Assert.Null(initial.VotedFor);

            await node.StartElectionAsync();

            RaftNodeStateSnapshot updated =
                await node.GetStateSnapshotAsync();

            Assert.Equal(RaftNodeState.Candidate, updated.State);
            Assert.Equal(1, updated.CurrentTerm);
            Assert.Equal("node-1", updated.VotedFor);

            // A previously captured snapshot must not change.
            Assert.Equal(RaftNodeState.Follower, initial.State);
            Assert.Equal(0, initial.CurrentTerm);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
