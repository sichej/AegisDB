using System.Globalization;
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Storage;
using AegisDB.Server.Distributed;

namespace AegisDB.Server.Monitoring;

/// <summary>
/// Hosts fixed-membership local Raft nodes with independent, replaceable instance lifetimes.
/// </summary>
public sealed class LocalRaftCluster(
    IConfiguration configuration,
    IHostEnvironment environment,
    ILogger<LocalRaftCluster> logger,
    IHostApplicationLifetime? applicationLifetime = null)
    : BackgroundService, IClusterMonitor, IDistributedKeyValueService, INodeLifecycleManager
{
    private readonly bool _enabled = configuration.GetValue<bool>("Raft:Enabled");
    private readonly object _sync = new();
    private readonly string[] _members = ["node-1", "node-2", "node-3"];
    private readonly LocalRaftTransport _transport = new();
    private readonly CancellationTokenSource _shutdown = new();
    private readonly TaskCompletionSource<Exception> _failure =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private IReadOnlyList<HostedNode> _nodes = [];
    private RaftKeyValueService? _keyValues;
    private string _directory = "";
    private bool _stopping;

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_enabled)
        {
            if (!environment.IsDevelopment() && !environment.IsEnvironment("Testing"))
                throw new InvalidOperationException("The in-process Raft cluster is only supported in Development or Testing.");

            _directory = Path.GetFullPath(configuration["Raft:DataDirectory"]
                ?? Path.Combine(environment.ContentRootPath, "data", "raft"));
            Directory.CreateDirectory(_directory);
            _nodes = _members.Select(id => new HostedNode(id)).ToArray();
            try
            {
                foreach (HostedNode slot in _nodes)
                    slot.Instance = await CreateInstanceAsync(slot.Id, cancellationToken);
                foreach (HostedNode slot in _nodes)
                    _transport.Register(slot.Instance!.Values.Node, slot.Instance.Lifetime);

                _keyValues = new RaftKeyValueService(GetNodeInstances, _transport);
                await base.StartAsync(cancellationToken);
                logger.LogInformation("Hosting three local Raft nodes in {Directory}. The /kv API remains independent.", _directory);
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Local cluster startup failed; draining created instances.");
                lock (_sync) _stopping = true;
                await Task.WhenAll(_nodes.Select(ShutdownNodeAsync));
                throw;
            }
        }
        else
        {
            await base.StartAsync(cancellationToken);
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_enabled) return;
        try
        {
            // Every initial node is registered before any runtime starts.
            lock (_sync)
            {
                foreach (HostedNode slot in _nodes)
                    StartServices(slot, slot.Instance!);
            }
            Exception failure = await _failure.Task.WaitAsync(stoppingToken);
            throw new InvalidOperationException("A local Raft component failed.", failure);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // The finally block owns shutdown, including concurrent admin transitions.
        }
        finally
        {
            lock (_sync) _stopping = true;
            await _shutdown.CancelAsync();
            await Task.WhenAll(_nodes.Select(ShutdownNodeAsync));
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        lock (_sync) _stopping = true;
        await _shutdown.CancelAsync();
        await base.StopAsync(cancellationToken);
    }

    private async Task<NodeInstance> CreateInstanceAsync(string id, CancellationToken cancellationToken)
    {
        var store = new KeyValueStore();
        var timer = new RaftElectionTimer(TimeSpan.FromMilliseconds(500), TimeSpan.FromMilliseconds(1000));
        var node = await RaftNode.CreateAsync(
            id, new RaftClusterConfiguration(_members),
            new RaftStateStore(Path.Combine(_directory, $"{id}-state.json")),
            new RaftLogStore(Path.Combine(_directory, $"{id}-log.json")),
            cancellationToken: cancellationToken, electionTimer: timer, stateMachine: new RaftStateMachine(store));
        string[] peers = _members.Where(member => member != id).ToArray();
        var lifetime = new NodeLifetime();
        var nodeTransport = _transport.ForNode(lifetime);
        var coordinator = new RaftElectionCoordinator(node, nodeTransport, peers);
        var election = new RaftElectionService(node, timer, coordinator);
        var heartbeat = new RaftHeartbeatService(node, nodeTransport, peers, TimeSpan.FromMilliseconds(100));
        return new NodeInstance(
            new RaftKeyValueNode(node, store, lifetime),
            lifetime, new RaftNodeRuntime(node, election, heartbeat, nodeTransport, peers));
    }

    private void StartServices(HostedNode slot, NodeInstance instance)
    {
        slot.Availability = "Running";
        instance.Services = Task.WhenAll(
            RunComponentAsync(slot, instance, "runtime", () => instance.Runtime.RunAsync(instance.Lifetime.Token)),
            RunComponentAsync(slot, instance, "application",
                () => new RaftStateMachineApplyService(instance.Values.Node).RunAsync(instance.Lifetime.Token)));
    }

    private async Task RunComponentAsync(HostedNode slot, NodeInstance instance, string component, Func<Task> run)
    {
        try
        {
            await run();
            if (!instance.Lifetime.Token.IsCancellationRequested)
                throw new InvalidOperationException($"Raft {component} for {slot.Id} stopped unexpectedly.");
        }
        catch (OperationCanceledException) when (instance.Lifetime.Token.IsCancellationRequested)
        {
            // Controlled node stop, not a failure of the cluster.
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "Raft {Component} for {NodeId} failed.", component, slot.Id);
            lock (_sync)
            {
                slot.Availability = "Faulted";
                instance.Lifetime.StopAccepting();
                _transport.Unregister(slot.Id);
            }
            await instance.Lifetime.CancelAsync();
            _failure.TrySetResult(exception);
            throw;
        }
    }

    public Task<NodeStatusResponse> StopNodeAsync(string nodeId, CancellationToken cancellationToken) =>
        ChangeNodeAsync(nodeId, NodeAction.Stop, cancellationToken);
    public Task<NodeStatusResponse> StartNodeAsync(string nodeId, CancellationToken cancellationToken) =>
        ChangeNodeAsync(nodeId, NodeAction.Start, cancellationToken);
    public Task<NodeStatusResponse> RestartNodeAsync(string nodeId, CancellationToken cancellationToken) =>
        ChangeNodeAsync(nodeId, NodeAction.Restart, cancellationToken);

    private async Task<NodeStatusResponse> ChangeNodeAsync(
        string nodeId, NodeAction action, CancellationToken cancellationToken)
    {
        if (!_enabled)
            throw new NodeLifecycleException(503, "cluster_disabled", "The local cluster is disabled.");
        HostedNode slot = _nodes.FirstOrDefault(node => node.Id == nodeId)
            ?? throw new NodeLifecycleException(404, "node_not_found", "No local cluster member has this ID.");
        bool entered = false;
        try
        {
            if (!await slot.Gate.WaitAsync(0, cancellationToken))
                throw new NodeLifecycleException(409, "node_busy", "Another lifecycle operation is in progress for this node.");
            entered = true;
            lock (_sync)
            {
                if (_stopping || applicationLifetime?.ApplicationStopping.IsCancellationRequested == true)
                    throw new NodeLifecycleException(503, "cluster_stopping", "The local cluster is shutting down.");
            }
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken, _shutdown.Token,
                applicationLifetime?.ApplicationStopping ?? CancellationToken.None);
            linked.Token.ThrowIfCancellationRequested();
            if (action is NodeAction.Stop or NodeAction.Restart)
                await StopNodeCoreAsync(slot);
            if (action is NodeAction.Start or NodeAction.Restart)
                await StartNodeCoreAsync(slot, linked.Token);
            return await ObserveNodeAsync(slot, linked.Token);
        }
        catch (OperationCanceledException exception)
        {
            bool shuttingDown = _shutdown.IsCancellationRequested ||
                applicationLifetime?.ApplicationStopping.IsCancellationRequested == true;
            throw new NodeLifecycleException(
                shuttingDown ? 503 : 408,
                shuttingDown ? "cluster_stopping" : "lifecycle_cancelled",
                "The lifecycle request was cancelled. An admitted stop still drains fully; refresh node status before retrying.",
                exception);
        }
        catch (Exception exception) when (exception is not NodeLifecycleException)
        {
            logger.LogError(exception, "Node {NodeId} lifecycle action {Action} failed.", nodeId, action);
            throw new NodeLifecycleException(500, "node_transition_failed",
                "The node transition failed. Inspect its availability and server logs before retrying.", exception);
        }
        finally
        {
            if (entered) slot.Gate.Release();
        }
    }

    private async Task StartNodeCoreAsync(HostedNode slot, CancellationToken cancellationToken)
    {
        lock (_sync)
        {
            if (slot.Availability == "Running") return;
        }
        await StopNodeCoreAsync(slot);
        lock (_sync) slot.Availability = "Starting";
        NodeInstance? instance = null;
        bool published = false;
        try
        {
            instance = await CreateInstanceAsync(slot.Id, cancellationToken);
            lock (_sync)
            {
                if (_stopping || cancellationToken.IsCancellationRequested)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    throw new OperationCanceledException(_shutdown.Token);
                }
                _transport.Register(instance.Values.Node, instance.Lifetime);
                slot.Instance = instance;
                published = true;
                StartServices(slot, instance);
            }
            if (instance.Services.IsCompleted) await instance.Services;
        }
        catch (Exception exception)
        {
            if (published)
                await StopNodeCoreAsync(slot);
            else if (instance is not null)
            {
                instance.Lifetime.StopAccepting();
                await instance.Lifetime.CancelAsync();
                instance.Lifetime.Dispose();
            }
            lock (_sync) slot.Availability = exception is OperationCanceledException ? "Stopped" : "Faulted";
            throw;
        }
    }

    private async Task StopNodeCoreAsync(HostedNode slot)
    {
        NodeInstance? instance;
        lock (_sync)
        {
            instance = slot.Instance;
            if (instance is null || instance.IsDrained)
            {
                slot.Availability = "Stopped";
                return;
            }
            slot.Availability = "Stopping";
            instance.Lifetime.StopAccepting();
            _transport.Unregister(slot.Id);
        }
        try
        {
            // Once stop begins, client cancellation must not release the files to a new instance early.
            await instance.Lifetime.CancelAsync();
            await Task.WhenAll(instance.Services, instance.Lifetime.Drained);
        }
        catch (Exception exception)
        {
            lock (_sync) slot.Availability = "Faulted";
            logger.LogError(exception, "Failed to drain node {NodeId}.", slot.Id);
            throw new NodeLifecycleException(500, "node_stop_failed", "The node did not stop cleanly; inspect server logs.", exception);
        }
        finally
        {
            if (instance.Services.IsCompleted && instance.Lifetime.Drained.IsCompleted)
            {
                instance.IsDrained = true;
                instance.Lifetime.Dispose();
            }
        }
        lock (_sync) slot.Availability = "Stopped";
    }

    private async Task ShutdownNodeAsync(HostedNode slot)
    {
        await slot.Gate.WaitAsync();
        try { await StopNodeCoreAsync(slot); }
        finally { slot.Gate.Release(); }
    }

    internal IReadOnlyList<RaftKeyValueNode> GetNodeInstances()
    {
        lock (_sync) return _nodes.Where(slot => slot.Instance is not null)
            .Select(slot => slot.Instance!.Values).ToArray();
    }

    private async Task<NodeStatusResponse> ObserveNodeAsync(HostedNode slot, CancellationToken cancellationToken)
    {
        NodeInstance instance;
        string availability;
        lock (_sync)
        {
            instance = slot.Instance ?? throw new InvalidOperationException("The node has not been initialized.");
            availability = slot.Availability;
        }
        RaftNodeStateSnapshot state = await instance.Values.Node.GetStateSnapshotAsync(cancellationToken);
        return new NodeStatusResponse(
            state.NodeId, state.State.ToString(),
            state.CurrentTerm.ToString(CultureInfo.InvariantCulture),
            state.CommitIndex.ToString(CultureInfo.InvariantCulture),
            state.LastApplied.ToString(CultureInfo.InvariantCulture),
            state.LastLogIndex.ToString(CultureInfo.InvariantCulture),
            state.LastLogTerm.ToString(CultureInfo.InvariantCulture), availability);
    }

    public async Task<ClusterResponse> GetClusterAsync(CancellationToken cancellationToken)
    {
        var statuses = new List<NodeStatusResponse>();
        foreach (HostedNode slot in _nodes)
            statuses.Add(await ObserveNodeAsync(slot, cancellationToken));
        return new ClusterResponse(_enabled, _enabled ? "local-in-process" : "disabled",
            "Availability describes local runtime tasks, not network reachability or quorum health. "
                + "Node snapshots are individually consistent, not a cluster-wide atomic snapshot. "
                + "Stopped nodes retain their last observed role and indices.",
            DateTimeOffset.UtcNow, statuses);
    }

    public Task<DistributedWriteResponse> PutAsync(string key, string value, CancellationToken cancellationToken) =>
        RequireKeyValues().PutAsync(key, value, cancellationToken);
    public Task<DistributedWriteResponse> DeleteAsync(string key, CancellationToken cancellationToken) =>
        RequireKeyValues().DeleteAsync(key, cancellationToken);
    public Task<DistributedReadResponse> GetAsync(string key, CancellationToken cancellationToken) =>
        RequireKeyValues().GetAsync(key, cancellationToken);
    private RaftKeyValueService RequireKeyValues() =>
        _keyValues ?? throw new DistributedOperationException(
            503, "cluster_disabled", "The local Raft cluster is disabled.", "not-submitted");

    public override void Dispose()
    {
        base.Dispose();
        _shutdown.Dispose();
    }

    private enum NodeAction { Stop, Start, Restart }
    private sealed class HostedNode(string id)
    {
        public string Id { get; } = id;
        public SemaphoreSlim Gate { get; } = new(1, 1);
        public NodeInstance? Instance { get; set; }
        public string Availability { get; set; } = "Starting";
    }
    private sealed class NodeInstance(RaftKeyValueNode values, NodeLifetime lifetime, RaftNodeRuntime runtime)
    {
        public RaftKeyValueNode Values { get; } = values;
        public NodeLifetime Lifetime { get; } = lifetime;
        public RaftNodeRuntime Runtime { get; } = runtime;
        public Task Services { get; set; } = Task.CompletedTask;
        public bool IsDrained { get; set; }
    }
}
