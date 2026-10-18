using System.Text;
using AegisDB.Core.Persistence;
using AegisDB.Core.Storage;

namespace AegisDB.Tests;

public class RecoveryManagerTests
{
    [Fact]
    public async Task RecoverAsync_ShouldRestoreDatabaseState()
    {
        string filePath = Path.Combine(
            Path.GetTempPath(),
            $"{Guid.NewGuid()}.wal"
        );

        try
        {
            var wal = new WriteAheadLog(filePath);

            await wal.AppendAsync(new WalRecord(
                WalOperation.Put,
                "user:1",
                Encoding.UTF8.GetBytes("Alice")));

            await wal.AppendAsync(new WalRecord(
                WalOperation.Put,
                "user:2",
                Encoding.UTF8.GetBytes("Bob")));

            await wal.AppendAsync(new WalRecord(
                WalOperation.Delete,
                "user:1",
                null));

            // Simulate a restart by creating a fresh in-memory store.
            var restoredStore = new KeyValueStore();
            var recovery = new RecoveryManager(wal, restoredStore);

            await recovery.RecoverAsync();

            Assert.Null(restoredStore.Get("user:1"));

            Assert.Equal(
                "Bob",
                Encoding.UTF8.GetString(
                    restoredStore.Get("user:2")!));
        }
        finally
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
    }
}