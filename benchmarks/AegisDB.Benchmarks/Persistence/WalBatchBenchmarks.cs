
using AegisDB.Core.Persistence;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;

namespace AegisDB.Benchmarks.Persistence;

[MemoryDiagnoser]
[SimpleJob(
  RunStrategy.Monitoring,
  launchCount: 1,
  warmupCount: 3,
  iterationCount: 10)]
public class WalBatchBenchmarks
{
    private string _filePath = null!;
    private WriteAheadLog _wal = null!;
    private WalRecord _record = null!;

    [Params(128, 1024, 16384)]
    public int ValueSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _filePath = Path.Combine(
          Path.GetTempPath(),
          $"aegisdb-batch-{Guid.NewGuid():N}.wal");

        byte[] value = new byte[ValueSize];
        new Random(42).NextBytes(value);

        _record = new WalRecord(
          WalOperation.Put,
          "benchmark-key",
          value);

        _wal = new WriteAheadLog(_filePath);
    }

    [IterationSetup]
    public void ResetWal()
    {
        File.WriteAllBytes(_filePath, []);
    }

    [Benchmark(OperationsPerInvoke = 100)]
    public async Task AppendBatch100()
    {
        for (int i = 0; i < 100; i++)
        {
            await _wal.AppendAsync(_record);
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        if (File.Exists(_filePath))
            File.Delete(_filePath);
    }
}
