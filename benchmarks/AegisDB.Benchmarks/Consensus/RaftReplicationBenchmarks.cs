
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Consensus.Transport;
using AegisDB.Core.Persistence;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;

namespace AegisDB.Benchmarks.Consensus;

[MemoryDiagnoser]
[SimpleJob(
  RunStrategy.Monitoring,
  launchCount: 1,
  warmupCount: 3,
  iterationCount: 20)]
public class RaftReplicationBenchmarks
{
    private static readonly string[] Members =
    [
      "node-1",
    "node-2",
    "node-3"
    ];

    private string _directory = null!;
    private string _iterationDirectory = null!;

    private RaftNode _leader = null!;
    private RaftReplicationManager _manager = null!;
    private WalRecord _command = null!;

    [Params(false, true)]
    public bool OneFollowerOffline { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _directory = Path.Combine(
          Path.GetTempPath(),
          $"aegisdb-replication-benchmark-{Guid.NewGuid():N}");

        Directory.CreateDirectory(_directory);

        byte[] value = new byte[1024];
        new Random(42).NextBytes(value);

        _command = new WalRecord(
          WalOperation.Put,
          "benchmark-key",
          value);
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _iterationDirectory = Path.Combine(
          _directory,
          Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(_iterationDirectory);

        var cluster = new RaftClusterConfiguration(Members);
        var transport = new InMemoryRaftTransport();

        var nodes = new Dictionary<string, RaftNode>();

        foreach (string nodeId in Members)
        {
            var stateStore = new RaftStateStore(
              Path.Combine(
                _iterationDirectory,
                $"{nodeId}-state.json"));

            var logStore = new RaftLogStore(
              Path.Combine(
                _iterationDirectory,
                $"{nodeId}-log.json"));

            var node = new RaftNode(
              nodeId,
              cluster,
              stateStore: stateStore,
              logStore: logStore);

            nodes.Add(nodeId, node);
            transport.Register(node);
        }

        _leader = nodes["node-1"];

        // Elect node-1 before the measured operation.
        // The three-node cluster requires two votes.
        _leader.StartElectionAsync()
          .GetAwaiter().GetResult();

        bool elected = _leader.ReceiveVoteAsync(
          voterId: "node-2",
          term: _leader.CurrentTerm,
          voteGranted: true,
          electionTerm: _leader.CurrentTerm)
          .GetAwaiter().GetResult();

        if (!elected || _leader.State != RaftNodeState.Leader)
        {
            throw new InvalidOperationException(
              "Failed to elect benchmark leader.");
        }

        // Create replication progress before appending.
        _manager = new RaftReplicationManager(
          _leader,
          transport,
          ["node-2", "node-3"]);

        if (OneFollowerOffline)
        {
            // Simulate an unreachable follower while preserving quorum.
            transport.Unregister("node-3");
        }
    }

    [Benchmark]
    public async Task<bool> AppendReplicateAndCommit()
    {
        RaftLogEntry entry =
          await _leader.AppendLeaderCommandAsync(_command);

        IReadOnlyDictionary<string, bool> results =
          await _manager.ReplicateAllOnceAsync();

        bool committed =
          await _manager.CommitAndApplyAsync();

        if (!results["node-2"] ||
            (OneFollowerOffline
              ? results["node-3"]
              : !results["node-3"]))
        {
            throw new InvalidOperationException(
              "Unexpected replication results.");
        }

        if (!committed ||
            _leader.CommitIndex < entry.Index ||
            _leader.LastApplied < entry.Index)
        {
            throw new InvalidOperationException(
              "Command was not committed and applied.");
        }

        return committed;
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        if (Directory.Exists(_iterationDirectory))
        {
            Directory.Delete(
              _iterationDirectory,
              recursive: true);
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(
              _directory,
              recursive: true);
        }
    }
}
