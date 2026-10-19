using System.Text;
using AegisDB.Core.Persistence;
using AegisDB.Tests.Helpers;

namespace AegisDB.Tests;

public sealed class TestFaultInjectorTests
{
    [Fact]
    public async Task AppendAsync_ShouldPauseUntilResumed()
    {
        string filePath = Path.Combine(
            Path.GetTempPath(),
            $"{Guid.NewGuid():N}.wal"
        );

        var injector = new TestFaultInjector("wal.before_flush");
        var wal = new WriteAheadLog(filePath, injector);

        try
        {
            var record = new WalRecord(
                WalOperation.Put,
                "user:1",
                Encoding.UTF8.GetBytes("Alice")
            );

            // Start the append without awaiting its completion.
            Task appendTask = wal.AppendAsync(record);

            using var timeout = new CancellationTokenSource(
                TimeSpan.FromSeconds(10)
            );

            // Wait until the WAL reaches the configured checkpoint.
            await injector.WaitForCheckpointAsync(timeout.Token);

            // The append is still blocked before the explicit flush.
            Assert.False(appendTask.IsCompleted);

            // Allow the operation to complete.
            injector.Resume();

            await appendTask;

            var records = await wal.ReadAllAsync();

            Assert.Single(records);
            Assert.Equal("user:1", records[0].Key);
        }
        finally
        {
            // Ensure blocked operations can resume even if
            // an assertion fails.
            injector.Resume();

            if (File.Exists(filePath))
                File.Delete(filePath);
        }
    }
}