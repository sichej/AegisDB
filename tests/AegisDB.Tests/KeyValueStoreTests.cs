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
}