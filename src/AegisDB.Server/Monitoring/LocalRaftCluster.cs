using System.Globalization;
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Consensus.Transport;
using AegisDB.Core.Storage;
using AegisDB.Server.Distributed;

namespace AegisDB.Server.Monitoring;

/// <summary>
/// Hosts an opt-in development cluster using real Raft nodes in one process.
/// Its storage and state machines are independent of the existing HTTP key-value API.
/// </summary>
public sealed class LocalRaftCluster(
    IConfiguration configuration,
    IHostEnvironment environment,
    ILogger<LocalRaftCluster> logger) : BackgroundService, IClusterMonitor, IDistributedKeyValueService
{
    private readonly bool _enabled = configuration.GetValue<bool>("Raft:Enabled");
    private IReadOnlyList<HostedNode> _nodes = [];
    private RaftKeyValueService? _keyValues;

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_enabled)
        {
            if (!environment.IsDevelopment() && !environment.IsEnvironment("Testing"))
            {
                throw new InvalidOperationException(
                    "The in-process Raft cluster is only supported in Development or Testing.");
            }

            string directory = Path.GetFullPath(
                configuration["Raft:DataDirectory"]
                ?? Path.Combine(environment.ContentRootPath, "data", "raft"));

            Directory.CreateDirectory(directory);

            string[] members = ["node-1", "node-2", "node-3"];
            var cluster = new RaftClusterConfiguration(members);
            var transport = new InMemoryRaftTransport();
            var nodes = new List<HostedNode>();

            foreach (string nodeId in members)
            {
                var store = new KeyValueStore();
                var timer = new RaftElectionTimer(
                    TimeSpan.FromMilliseconds(500),
                    TimeSpan.FromMilliseconds(1000));

                var node = await RaftNode.CreateAsync(
                    nodeId,
                    cluster,
                    new RaftStateStore(Path.Combine(directory, $"{nodeId}-state.json")),
                    new RaftLogStore(Path.Combine(directory, $"{nodeId}-log.json")),
                    cancellationToken: cancellationToken,
                    electionTimer: timer,
                    stateMachine: new RaftStateMachine(store));

                transport.Register(node);

                string[] peers = members.Where(id => id != nodeId).ToArray();
                var coordinator = new RaftElectionCoordinator(node, transport, peers);
                var election = new RaftElectionService(node, timer, coordinator);
                var heartbeat = new RaftHeartbeatService(
                    node, transport, peers, TimeSpan.FromMilliseconds(100));

                nodes.Add(new HostedNode(
                    node,
                    store,
                    new RaftNodeRuntime(node, election, heartbeat, transport, peers)));
            }

            _nodes = nodes;
            _keyValues = new RaftKeyValueService(
                nodes.Select(node => new RaftKeyValueNode(node.Node, node.Store)).ToArray(),
                transport);
            logger.LogInformation(
                "Hosting three local Raft nodes in {Directory}. The /kv API remains independent.",
                directory);
        }

        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_enabled)
            return;

        using var clusterCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

        foreach (HostedNode node in _nodes)
        {
            node.RuntimeTask = Task.WhenAll(
                RunComponentAsync(node.Node.NodeId, "runtime",
                    () => node.Runtime.RunAsync(clusterCancellation.Token), clusterCancellation),
                RunComponentAsync(node.Node.NodeId, "application",
                    () => new RaftStateMachineApplyService(node.Node).RunAsync(clusterCancellation.Token),
                    clusterCancellation));
        }

        await Task.WhenAll(_nodes.Select(node => node.RuntimeTask!));
    }

    private async Task RunComponentAsync(
        string nodeId,
        string component,
        Func<Task> run,
        CancellationTokenSource clusterCancellation)
    {
        try
        {
            await run();

            if (!clusterCancellation.IsCancellationRequested)
            {
                throw new InvalidOperationException(
                    $"Raft {component} for {nodeId} stopped unexpectedly.");
            }
        }
        catch (OperationCanceledException) when (clusterCancellation.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "Raft {Component} for {NodeId} failed.", component, nodeId);
            await clusterCancellation.CancelAsync();
            throw;
        }
    }

    public async Task<ClusterResponse> GetClusterAsync(
        CancellationToken cancellationToken)
    {
        var statuses = new List<NodeStatusResponse>();

        foreach (HostedNode node in _nodes)
        {
            RaftNodeStateSnapshot snapshot =
                await node.Node.GetStateSnapshotAsync(cancellationToken);

            Task? runtime = node.RuntimeTask;
            string availability = runtime is null ? "Starting"
                : runtime.IsFaulted ? "Faulted"
                : runtime.IsCompleted ? "Stopped"
                : "Running";

            statuses.Add(new NodeStatusResponse(
                snapshot.NodeId,
                snapshot.State.ToString(),
                snapshot.CurrentTerm.ToString(CultureInfo.InvariantCulture),
                snapshot.CommitIndex.ToString(CultureInfo.InvariantCulture),
                snapshot.LastApplied.ToString(CultureInfo.InvariantCulture),
                snapshot.LastLogIndex.ToString(CultureInfo.InvariantCulture),
                snapshot.LastLogTerm.ToString(CultureInfo.InvariantCulture),
                availability));
        }

        return new ClusterResponse(
            _enabled,
            _enabled ? "local-in-process" : "disabled",
            "Availability describes local runtime tasks, not network reachability or quorum health. "
                + "Node snapshots are individually consistent, not a cluster-wide atomic snapshot.",
            DateTimeOffset.UtcNow,
            statuses);
    }

    public Task<DistributedWriteResponse> PutAsync(
        string key, string value, CancellationToken cancellationToken) =>
        RequireKeyValues().PutAsync(key, value, cancellationToken);

    public Task<DistributedWriteResponse> DeleteAsync(
        string key, CancellationToken cancellationToken) =>
        RequireKeyValues().DeleteAsync(key, cancellationToken);

    public Task<DistributedReadResponse> GetAsync(
        string key, CancellationToken cancellationToken) =>
        RequireKeyValues().GetAsync(key, cancellationToken);

    private RaftKeyValueService RequireKeyValues() =>
        _keyValues ?? throw new DistributedOperationException(
            503, "cluster_disabled", "The local Raft cluster is disabled.", "not-submitted");

    private sealed class HostedNode(RaftNode node, KeyValueStore store, RaftNodeRuntime runtime)
    {
        public RaftNode Node { get; } = node;
        public KeyValueStore Store { get; } = store;
        public RaftNodeRuntime Runtime { get; } = runtime;
        public Task? RuntimeTask { get; set; }
    }
}
