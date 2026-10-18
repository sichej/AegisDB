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
}