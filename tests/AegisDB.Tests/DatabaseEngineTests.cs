using System.Text;
using AegisDB.Core;
using AegisDB.Core.Persistence;
using AegisDB.Core.Storage;

namespace AegisDB.Tests;

public class DatabaseEngineTests
{
    [Fact]
    public async Task Database_ShouldRestorePersistedDataAfterRestart()
    {
        string filePath = Path.Combine(
            Path.GetTempPath(),
            $"{Guid.NewGuid()}.wal"
        );

        try
        {
            // Create the initial database instance.
            var wal = new WriteAheadLog(filePath);
            var store = new KeyValueStore();
            var database = new DatabaseEngine(store, wal);

            await database.PutAsync(
                "user:1",
                Encoding.UTF8.GetBytes("Alice")
            );

            // Simulate a restart by creating a new in-memory store.
            var restoredStore = new KeyValueStore();
            var restoredWal = new WriteAheadLog(filePath);

            var recovery = new RecoveryManager(
                restoredWal,
                restoredStore
            );

            await recovery.RecoverAsync();

            var restoredDatabase = new DatabaseEngine(
                restoredStore,
                restoredWal
            );

            byte[]? result = restoredDatabase.Get("user:1");

            Assert.NotNull(result);
            Assert.Equal("Alice", Encoding.UTF8.GetString(result));
        }
        finally
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
    }
}