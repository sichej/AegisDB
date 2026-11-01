
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Persistence;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;

namespace AegisDB.Benchmarks.Consensus;

[MemoryDiagnoser]
[SimpleJob(
  RunStrategy.Monitoring,
  launchCount: 1,
  warmupCount: 3,
  iterationCount: 10)]
public class RaftLogAppendBenchmarks
{
    private string _directory = null!;
    private RaftStateStore _stateStore = null!;
    private RaftLogStore _logStore = null!;
    private RaftClusterConfiguration _cluster = null!;
    private RaftNode _leader = null!;
    private RaftLogEntry[] _initialEntries = null!;
    private WalRecord _command = null!;

    [Params(0, 100, 1000, 10000)]
    public int InitialLogEntries { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _directory = Path.Combine(
          Path.GetTempPath(),
          $"aegisdb-raft-benchmark-{Guid.NewGuid():N}");

        Directory.CreateDirectory(_directory);

        _stateStore = new RaftStateStore(
          Path.Combine(_directory, "raft-state.json"));

        _logStore = new RaftLogStore(
          Path.Combine(_directory, "raft-log.json"));

        _cluster = new RaftClusterConfiguration(["node-1"]);

        byte[] value = new byte[128];
        new Random(42).NextBytes(value);

        _initialEntries = new RaftLogEntry[InitialLogEntries];

        for (int i = 0; i < InitialLogEntries; i++)
        {
            _initialEntries[i] = new RaftLogEntry(
              Index: i + 1L,
              Term: 1,
              Command: new WalRecord(
                WalOperation.Put,
                $"seed:{i:D8}",
                value));
        }

        _command = new WalRecord(
          WalOperation.Put,
          "benchmark-key",
          value);
    }

    [IterationSetup]
    public void IterationSetup()
    {
        // Reset persistent state to the same baseline.
        // This preparation is excluded from benchmark timing.
        _stateStore.SaveAsync(
          new RaftPersistentState(
            CurrentTerm: 1,
            VotedFor: null))
          .GetAwaiter().GetResult();

        _logStore.SaveAsync(_initialEntries)
          .GetAwaiter().GetResult();

        // Restore the persisted log and elect the node
        // as a single-member cluster leader.
        _leader = RaftNode.CreateAsync(
          "node-1",
          _cluster,
          _stateStore,
          _logStore)
          .GetAwaiter().GetResult();

        _leader.StartElectionAsync()
          .GetAwaiter().GetResult();

        _leader.ReceiveSingleNodeMajorityAsync()
          .GetAwaiter().GetResult();
    }

    [Benchmark]
    public Task<RaftLogEntry> Append()
    {
        return _leader.AppendLeaderCommandAsync(_command);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }
}
