
using AegisDB.Core.Storage;
using BenchmarkDotNet.Attributes;

namespace AegisDB.Benchmarks.Storage;

[MemoryDiagnoser]
public class KeyValueStoreBenchmarks
{
    private KeyValueStore _store = null!;
    private string[] _keys = null!;
    private byte[] _value = null!;
    private long _cursor;

    [Params(128, 1024, 16384)]
    public int ValueSize { get; set; }

    [Params(1000, 10000)]
    public int KeyCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _store = new KeyValueStore();
        _keys = new string[KeyCount];
        _value = new byte[ValueSize];

        var random = new Random(42);
        random.NextBytes(_value);

        for (int i = 0; i < KeyCount; i++)
        {
            string key = $"key:{i:D8}";
            _keys[i] = key;

            _store.Put(key, _value);
        }

        _cursor = 0;
    }

    private string NextKey()
    {
        long index = _cursor++ % KeyCount;
        return _keys[(int)index];
    }

    [Benchmark(Baseline = true)]
    public byte[]? Get()
    {
        return _store.Get(NextKey());
    }

    [Benchmark]
    public void Put()
    {
        // Update an existing key without growing the dataset.
        _store.Put(NextKey(), _value);
    }

    [Benchmark]
    public bool DeleteAndRestore()
    {
        string key = NextKey();

        bool deleted = _store.Delete(key);

        // Restore the key to keep the dataset size stable.
        _store.Put(key, _value);

        return deleted;
    }

    [Benchmark(OperationsPerInvoke = 100)]
    public long MixedWorkload()
    {
        long result = 0;

        // One batch consists of 100 logical operations:
        // 80 reads, 15 updates, and 5 delete operations.
        // Each delete is followed by an additional restore.
        for (int i = 0; i < 100; i++)
        {
            string key = NextKey();
            int operation = i % 20;

            if (operation < 16)
            {
                byte[]? value = _store.Get(key);
                result += value?.Length ?? 0;
            }
            else if (operation < 19)
            {
                _store.Put(key, _value);
            }
            else
            {
                if (_store.Delete(key))
                    result++;

                _store.Put(key, _value);
            }
        }

        return result;
    }
}
