using AegisDB.Core.Storage;

namespace AegisDB.Tests;

public class KeyValueStoreTests
{
    [Fact]
    public async Task ConcurrentWrites_ShouldPreserveAllKeys()
    {
        var store = new KeyValueStore();
        const int numberOfWrites = 1000;

        var tasks = Enumerable.Range(0, numberOfWrites)
            .Select(i => Task.Run(() =>
            {
                store.Put($"key:{i}", BitConverter.GetBytes(i));
            }));

        await Task.WhenAll(tasks);

        for (int i = 0; i < numberOfWrites; i++)
        {
            var value = store.Get($"key:{i}");

            Assert.NotNull(value);
            Assert.Equal(i, BitConverter.ToInt32(value!));
        }
    }

    [Fact]
    public void Put_ShouldCopyInputArray()
    {
        var store = new KeyValueStore();
        byte[] original = { 1, 2, 3 };

        store.Put("key:1", original);

        // Modify the original array after storing it
        original[0] = 99;

        var stored = store.Get("key:1");

        Assert.NotNull(stored);
        Assert.Equal(1, stored[0]);
    }

    [Fact]
    public void Get_ShouldReturnCopyOfStoredArray()
    {
        var store = new KeyValueStore();
        store.Put("key:1", new byte[] { 1, 2, 3 });

        var retrieved = store.Get("key:1");

        Assert.NotNull(retrieved);

        // Modify the returned array
        retrieved[0] = 99;

        // Retrieve the value again
        var stored = store.Get("key:1");

        Assert.NotNull(stored);
        Assert.Equal(1, stored[0]);
    }

    [Fact]
    public void CreateSnapshot_ShouldReturnIndependentCopy()
    {
        var store = new KeyValueStore();

        store.Put("user:1", new byte[] { 1, 2, 3 });

        var snapshot = store.CreateSnapshot();

        // Modify the original database after taking the snapshot.
        store.Put("user:1", new byte[] { 4, 5, 6 });
        store.Put("user:2", new byte[] { 7, 8, 9 });

        // The snapshot must preserve the original state.
        Assert.Single(snapshot);
        Assert.Equal(new byte[] { 1, 2, 3 }, snapshot["user:1"]);

        // Modifying the snapshot must not affect the database.
        snapshot["user:1"][0] = 99;

        Assert.Equal(
            new byte[] { 4, 5, 6 },
            store.Get("user:1")
        );
    }

    [Fact]
    public void RestoreSnapshot_ShouldReplaceExistingState()
    {
        var store = new KeyValueStore();

        store.Put("old:key", new byte[] { 99 });

        var snapshot = new Dictionary<string, byte[]>
        {
            ["user:1"] = new byte[] { 1, 2, 3 },
            ["user:2"] = new byte[] { 4, 5, 6 }
        };

        store.RestoreSnapshot(snapshot);

        // The previous state must be completely replaced.
        Assert.Null(store.Get("old:key"));

        Assert.Equal(
            new byte[] { 1, 2, 3 },
            store.Get("user:1")
        );

        Assert.Equal(
            new byte[] { 4, 5, 6 },
            store.Get("user:2")
        );

        // Verify that restored values do not share external buffers.
        snapshot["user:1"][0] = 99;

        Assert.Equal(
            new byte[] { 1, 2, 3 },
            store.Get("user:1")
        );
    }
}