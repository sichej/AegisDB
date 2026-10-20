
using AegisDB.Core.Consensus.Persistence;

namespace AegisDB.Tests;

public sealed class RaftStateStoreTests
{
    [Fact]
    public async Task SaveAndLoad_ShouldRestorePersistedElectionState()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            string path = Path.Combine(directory, "raft-state.json");

            var writer = new RaftStateStore(path);

            await writer.SaveAsync(
                new RaftPersistentState(
                    CurrentTerm: 5,
                    VotedFor: "node-2"));

            // Simulate a new component opening the same persisted state.
            var reader = new RaftStateStore(path);

            var restored = await reader.LoadAsync();

            Assert.Equal(5, restored.CurrentTerm);
            Assert.Equal("node-2", restored.VotedFor);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
