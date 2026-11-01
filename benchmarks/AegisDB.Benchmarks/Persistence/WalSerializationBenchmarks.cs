
using AegisDB.Core.Persistence;
using BenchmarkDotNet.Attributes;

namespace AegisDB.Benchmarks.Persistence;

[MemoryDiagnoser]
public class WalSerializationBenchmarks
{
  private WalRecord _record = null!;

  [Params(128, 1024, 16384)]
  public int ValueSize { get; set; }

  [GlobalSetup]
  public void Setup()
  {
    byte[] value = new byte[ValueSize];
    new Random(42).NextBytes(value);

    _record = new WalRecord(
      WalOperation.Put,
      "benchmark-key",
      value);
  }

  [Benchmark]
  public byte[] Serialize()
  {
    return WalSerializer.Serialize(_record);
  }
}
