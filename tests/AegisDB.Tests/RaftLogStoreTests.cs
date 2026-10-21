
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Persistence;

namespace AegisDB.Tests;

public sealed class RaftLogStoreTests
{
    [Fact]
    public async Task SaveAndLoad_ShouldRestoreLogEntries()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        try
        {
            string path = Path.Combine(directory, "raft-log.json");

            var entries = new[]
            {
                new RaftLogEntry(
                    1,
                    1,
                    new WalRecord(
                        WalOperation.Put,
                        "user:1",
                        "Alice"u8.ToArray())),

                new RaftLogEntry(
                    2,
                    1,
                    new WalRecord(
                        WalOperation.Delete,
                        "user:1",
                        null))
            };

            var writer = new RaftLogStore(path);

            await writer.SaveAsync(entries);

            // Simulate a new component loading the persisted log.
            var reader = new RaftLogStore(path);

            var restored = await reader.LoadAsync();

            Assert.Equal(2, restored.Count);
            Assert.Equal(1, restored[0].Index);
            Assert.Equal(2, restored[1].Index);

            Assert.Equal(
                "Alice",
                System.Text.Encoding.UTF8.GetString(
                    restored[0].Command.Value!));

            Assert.Equal(
                WalOperation.Delete,
                restored[1].Command.Operation);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
