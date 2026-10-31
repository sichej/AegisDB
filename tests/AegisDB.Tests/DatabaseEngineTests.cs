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

    [Fact]
    public async Task CreateSnapshotAsync_ShouldCaptureDataAndWalOffset()
    {
        string filePath = Path.Combine(
            Path.GetTempPath(),
            $"{Guid.NewGuid():N}.wal"
        );

        try
        {
            var store = new KeyValueStore();
            var wal = new WriteAheadLog(filePath);
            var database = new DatabaseEngine(store, wal);

            await database.PutAsync(
                "user:1",
                Encoding.UTF8.GetBytes("Alice")
            );

            Snapshot snapshot = await database.CreateSnapshotAsync();

            long expectedOffset = new FileInfo(filePath).Length;

            Assert.Equal(expectedOffset, snapshot.WalOffset);
            Assert.Equal(
                "Alice",
                Encoding.UTF8.GetString(snapshot.Data["user:1"])
            );

            // Subsequent mutations must not alter the snapshot.
            await database.PutAsync(
                "user:1",
                Encoding.UTF8.GetBytes("Bob")
            );

            Assert.Equal(
                "Alice",
                Encoding.UTF8.GetString(snapshot.Data["user:1"])
            );
        }
        finally
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
    }


    [Fact]
    public async Task PutAsync_ShouldNotRetainCallerOwnedBuffer()
    {
        string filePath = Path.Combine(
            Path.GetTempPath(),
            $"{Guid.NewGuid():N}.wal");

        try
        {
            var wal = new WriteAheadLog(filePath);
            var store = new KeyValueStore();
            var database = new DatabaseEngine(store, wal);

            byte[] originalValue = [1, 2, 3];

            await database.PutAsync("user:1", originalValue);

            // Mutate the original buffer after submission.
            originalValue[0] = 99;
            originalValue[1] = 88;

            byte[]? liveValue = database.Get("user:1");

            Assert.Equal(new byte[] { 1, 2, 3 }, liveValue);

            // Rebuild the store using only the persisted WAL.
            var recoveredStore = new KeyValueStore();
            var recoveredWal = new WriteAheadLog(filePath);

            var recovery = new RecoveryManager(
                recoveredWal,
                recoveredStore);

            await recovery.RecoverAsync();

            Assert.Equal(
                new byte[] { 1, 2, 3 },
                recoveredStore.Get("user:1"));

            Assert.Equal(liveValue, recoveredStore.Get("user:1"));
        }
        finally
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
    }


    [Fact]
    public async Task PutAsync_WhenCallerMutatesBufferWhileWaitingForLock_ShouldPersistOriginalValue()
    {
        string filePath = Path.Combine(
            Path.GetTempPath(),
            $"{Guid.NewGuid():N}.wal");

        try
        {
            var wal = new WriteAheadLog(filePath);
            var store = new KeyValueStore();
            var database = new DatabaseEngine(store, wal);

            byte[] originalValue = [1, 2, 3];
            byte[] expectedValue = [1, 2, 3];

            // Hold the engine's write lock to create a deterministic
            // suspension point before the pending PUT can proceed.
            var lockField = typeof(DatabaseEngine).GetField(
                "_writeLock",
                System.Reflection.BindingFlags.Instance |
                System.Reflection.BindingFlags.NonPublic);

            Assert.NotNull(lockField);

            var writeLock = Assert.IsType<SemaphoreSlim>(
                lockField.GetValue(database));

            await writeLock.WaitAsync();

            Task putTask;

            try
            {
                // The PUT must capture its own copy before awaiting the lock.
                putTask = database.PutAsync("user:1", originalValue);

                Assert.False(putTask.IsCompleted);

                // Mutate the caller-owned buffer while PUT is suspended.
                originalValue[0] = 99;
                originalValue[1] = 88;
                originalValue[2] = 77;
            }
            finally
            {
                writeLock.Release();
            }

            await putTask;

            // The live store must contain the value captured at submission.
            Assert.Equal(
                expectedValue,
                database.Get("user:1"));

            // Recover the database exclusively from the persisted WAL.
            var recoveredStore = new KeyValueStore();
            var recoveredWal = new WriteAheadLog(filePath);

            var recovery = new RecoveryManager(
                recoveredWal,
                recoveredStore);

            await recovery.RecoverAsync();

            // The recovered value must match the original submission.
            Assert.Equal(
                expectedValue,
                recoveredStore.Get("user:1"));

            // Live and recovered states must agree byte-for-byte.
            Assert.Equal(
                database.Get("user:1"),
                recoveredStore.Get("user:1"));
        }
        finally
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
    }

}