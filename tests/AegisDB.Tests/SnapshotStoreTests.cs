
using System.Text;
using AegisDB.Core.Persistence;

namespace AegisDB.Tests;

public sealed class SnapshotStoreTests
{
    [Fact]
    public async Task SaveAndLoad_ShouldPreserveSnapshot()
    {
        string filePath = Path.Combine(
            Path.GetTempPath(),
            $"{Guid.NewGuid():N}.snapshot"
        );

        try
        {
            var store = new SnapshotStore(filePath);

            var original = new Snapshot(
                WalOffset: 128,
                Data: new Dictionary<string, byte[]>
                {
                    ["user:1"] = Encoding.UTF8.GetBytes("Alice"),
                    ["user:2"] = Encoding.UTF8.GetBytes("Bob")
                }
            );

            await store.SaveAsync(original);

            Snapshot? restored = await store.LoadAsync();

            Assert.NotNull(restored);
            Assert.Equal(128, restored.WalOffset);
            Assert.Equal(2, restored.Data.Count);

            Assert.Equal(
                "Alice",
                Encoding.UTF8.GetString(restored.Data["user:1"])
            );

            Assert.Equal(
                "Bob",
                Encoding.UTF8.GetString(restored.Data["user:2"])
            );
        }
        finally
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
    }

    [Fact]
    public async Task LoadAsync_CorruptedSnapshot_ShouldThrow()
    {
        string filePath = Path.Combine(
            Path.GetTempPath(),
            $"{Guid.NewGuid():N}.snapshot"
        );

        try
        {
            var store = new SnapshotStore(filePath);

            var snapshot = new Snapshot(
                128,
                new Dictionary<string, byte[]>
                {
                    ["user:1"] = Encoding.UTF8.GetBytes("Alice")
                }
            );

            await store.SaveAsync(snapshot);

            byte[] bytes = await File.ReadAllBytesAsync(filePath);

            // Corrupt one payload byte without updating the checksum.
            bytes[^5] ^= 0xFF;

            await File.WriteAllBytesAsync(filePath, bytes);

            await Assert.ThrowsAsync<InvalidDataException>(
                () => store.LoadAsync()
            );
        }
        finally
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
    }
}
