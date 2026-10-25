using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using AegisDB.Core;
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Persistence;
using AegisDB.Core.Storage;
using AegisDB.Server.Distributed;
using AegisDB.Server.Monitoring;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AegisDB.Tests;

public sealed class NodeLifecycleTests
{
    [Fact]
    public async Task RestartRunningFollower_ShouldDrainAppliedInstanceAndRebuildFromLeader()
    {
        await using var host = await ClusterHost.StartAsync();
        NodeStatusResponse leader = await host.WaitForLeaderAsync();
        await host.PutAsync("retained", "Alice");
        await host.WaitForAppliedAsync(1);
        RaftKeyValueNode old = host.Instances.First(node => node.Node.NodeId != leader.NodeId);
        Assert.Equal("Alice", Text(old, "retained"));

        await host.ControlAsync(old.Node.NodeId, "restart");
        RaftKeyValueNode fresh = host.Instance(old.Node.NodeId);
        Assert.NotSame(old.Node, fresh.Node);
        Assert.NotSame(old.Store, fresh.Store);
        Assert.True(old.Lifetime!.Token.IsCancellationRequested);
        Assert.False(old.Lifetime.IsAvailable);
        await old.Lifetime.Drained;
        Assert.False(fresh.Lifetime!.Token.IsCancellationRequested);
        await host.WaitForAppliedAsync(1);
        Assert.Equal(1, fresh.Node.LastLogIndex);
        Assert.Equal("Alice", Text(fresh, "retained"));
    }

    [Fact]
    public async Task StopAndRestartFollower_ShouldPreserveFilesAndCatchUpUsingFreshInstance()
    {
        await using var host = await ClusterHost.StartAsync();
        NodeStatusResponse leader = await host.WaitForLeaderAsync();
        RaftKeyValueNode follower = host.Instances.First(node => node.Node.NodeId != leader.NodeId);
        string id = follower.Node.NodeId;
        await host.ControlAsync(id, "stop");
        var persistedState = await new RaftStateStore(host.StatePath(id)).LoadAsync();
        var persistedLog = await new RaftLogStore(host.LogPath(id)).LoadAsync();
        await host.PutAsync("while-stopped", "Alice");
        Assert.False(follower.Lifetime!.IsAvailable);
        Assert.True(follower.Lifetime.Token.IsCancellationRequested);
        Assert.Null(follower.Store.Get("while-stopped"));
        Assert.Equal(persistedLog.Count, follower.Node.LastLogIndex);
        Assert.Equal(persistedState, await new RaftStateStore(host.StatePath(id)).LoadAsync());

        await host.ControlAsync(id, "restart");
        RaftKeyValueNode fresh = host.Instance(id);
        Assert.NotSame(follower.Node, fresh.Node);
        Assert.NotSame(follower.Store, fresh.Store);
        Assert.NotSame(follower.Lifetime, fresh.Lifetime);
        Assert.False(fresh.Lifetime!.Token.IsCancellationRequested);
        Assert.True(fresh.Node.CurrentTerm >= persistedState.CurrentTerm);
        await host.WaitForAppliedAsync(1);
        Assert.Equal("Alice", Text(fresh, "while-stopped"));
        Assert.All(host.Instances, node =>
        {
            Assert.Equal(3, node.Node.Cluster.ClusterSize);
            Assert.Equal(2, node.Node.Cluster.QuorumSize);
        });
    }

    [Fact]
    public async Task StopLeader_ShouldElectReplacementAcceptPutAndRecoverOldLeaderState()
    {
        await using var host = await ClusterHost.StartAsync();
        NodeStatusResponse leader = await host.WaitForLeaderAsync();
        await host.PutAsync("before-failover", "Alice");
        await host.WaitForAppliedAsync(1);
        RaftKeyValueNode old = host.Instance(leader.NodeId);
        Assert.Equal("Alice", Text(old, "before-failover"));
        RaftLogEntry first = (await new RaftLogStore(host.LogPath(leader.NodeId)).LoadAsync()).Single();

        await host.ControlAsync(leader.NodeId, "stop");
        NodeStatusResponse replacement = await host.WaitForLeaderAsync(leader.NodeId, long.Parse(leader.CurrentTerm));
        Assert.NotEqual(leader.NodeId, replacement.NodeId);
        var written = await host.PutAsync("after-failover", "Bob");
        Assert.Equal(replacement.NodeId, written.LeaderId);
        Assert.Equal("2", written.LogIndex);
        await host.ControlAsync(leader.NodeId, "start");
        await host.WaitForAppliedAsync(2);

        RaftKeyValueNode fresh = host.Instance(leader.NodeId);
        Assert.NotSame(old.Node, fresh.Node);
        Assert.Equal("Alice", Text(fresh, "before-failover"));
        Assert.Equal("Bob", Text(fresh, "after-failover"));
        IReadOnlyList<RaftLogEntry> restored = await new RaftLogStore(host.LogPath(leader.NodeId)).LoadAsync();
        Assert.Equal(2, restored.Count);
        Assert.Equal(first.Index, restored[0].Index);
        Assert.Equal(first.Term, restored[0].Term);
        Assert.Equal(first.Command.Key, restored[0].Command.Key);
        Assert.Equal(first.Command.Value, restored[0].Command.Value);
    }

    [Fact]
    public async Task RestartPreviouslyAppliedStateMachine_ShouldStartEmptyThenReplayOnlyAfterCommitKnowledge()
    {
        await using var host = await ClusterHost.StartAsync();
        NodeStatusResponse leader = await host.WaitForLeaderAsync();
        await host.PutAsync("retained", "Alice");
        await host.PutAsync("removed", "temporary");
        using (var deletion = await host.Client.DeleteAsync("/admin/kv/removed"))
            Assert.Equal(HttpStatusCode.OK, deletion.StatusCode);
        await host.WaitForAppliedAsync(3);
        RaftKeyValueNode old = host.Instance(leader.NodeId);
        Assert.Equal("Alice", Text(old, "retained"));
        Assert.Null(old.Store.Get("removed"));
        foreach (RaftKeyValueNode node in host.Instances)
            await host.ControlAsync(node.Node.NodeId, "stop");
        var state = await new RaftStateStore(host.StatePath(leader.NodeId)).LoadAsync();
        var log = await new RaftLogStore(host.LogPath(leader.NodeId)).LoadAsync();

        await host.ControlAsync(leader.NodeId, "start");
        RaftKeyValueNode fresh = host.Instance(leader.NodeId);
        Assert.NotSame(old.Node, fresh.Node);
        Assert.NotSame(old.Store, fresh.Store);
        Assert.Equal(3, fresh.Node.LastLogIndex);
        Assert.Equal(0, fresh.Node.CommitIndex);
        Assert.Equal(0, fresh.Node.LastApplied);
        Assert.True(fresh.Node.CurrentTerm >= state.CurrentTerm);
        if (fresh.Node.CurrentTerm == state.CurrentTerm)
            Assert.Equal(state.VotedFor, fresh.Node.VotedFor);
        Assert.Null(fresh.Store.Get("retained"));
        Assert.Null(fresh.Store.Get("removed"));
        for (int i = 0; i < log.Count; i++)
        {
            RaftLogEntry entry = fresh.Node.Log.GetEntry(i + 1)!;
            Assert.Equal(log[i].Term, entry.Term);
            Assert.Equal(log[i].Command.Key, entry.Command.Key);
            Assert.Equal(log[i].Command.Operation, entry.Command.Operation);
            Assert.Equal(log[i].Command.Value, entry.Command.Value);
        }

        foreach (RaftKeyValueNode node in host.Instances.Where(node => node.Node.NodeId != leader.NodeId))
            await host.ControlAsync(node.Node.NodeId, "start");
        NodeStatusResponse newLeader = await host.WaitForLeaderAsync();
        Assert.Null(host.Instance(newLeader.NodeId).Store.Get("retained"));
        await host.PutAsync("current-term-command", "committed");
        await host.WaitForAppliedAsync(4);
        Assert.All(host.Instances, node =>
        {
            Assert.Equal("Alice", Text(node, "retained"));
            Assert.Null(node.Store.Get("removed"));
            Assert.Equal("committed", Text(node, "current-term-command"));
        });
    }

    [Fact]
    public async Task RepeatedAndConcurrentControls_ShouldBeIdempotentOrConflictAndDrainBeforeStart()
    {
        await using var host = await ClusterHost.StartAsync();
        NodeStatusResponse leader = await host.WaitForLeaderAsync();
        RaftKeyValueNode node = host.Instances.First(item => item.Node.NodeId != leader.NodeId);
        string id = node.Node.NodeId;
        using IDisposable lease = node.Lifetime!.TryAcquire()!;
        Task<HttpResponseMessage> stopping = host.Client.PostAsync($"/admin/nodes/{id}/stop", null);
        await host.WaitUntilAsync(async () => (await host.StatusAsync(id)).Availability == "Stopping");
        Assert.False(stopping.IsCompleted);
        Assert.False(node.Lifetime.IsAvailable);

        foreach (string action in new[] { "stop", "start", "restart" })
        {
            using var conflict = await host.Client.PostAsync($"/admin/nodes/{id}/{action}", null);
            await AssertProblemAsync(conflict, HttpStatusCode.Conflict, "node_busy");
            Assert.Same(node.Node, host.Instance(id).Node);
        }
        lease.Dispose();
        using var stopped = await stopping;
        Assert.Equal(HttpStatusCode.OK, stopped.StatusCode);
        await host.ControlAsync(id, "stop");
        await host.ControlAsync(id, "start");
        RaftKeyValueNode fresh = host.Instance(id);
        await host.ControlAsync(id, "start");
        Assert.Same(fresh.Node, host.Instance(id).Node);
        Assert.NotSame(node.Node, fresh.Node);
        await host.PutAsync("healthy-after-controls", "value");
        await host.WaitForAppliedAsync(1);
    }

    [Fact]
    public async Task CancelledStop_ShouldStillDrainAndLeaveNoRunningOldInstance()
    {
        await using var host = await ClusterHost.StartAsync();
        await host.WaitForLeaderAsync();
        RaftKeyValueNode node = host.Instances[0];
        using IDisposable lease = node.Lifetime!.TryAcquire()!;
        using var cancellation = new CancellationTokenSource();
        Task<NodeStatusResponse> stopping = host.Manager.StopNodeAsync(node.Node.NodeId, cancellation.Token);
        cancellation.Cancel();
        Assert.False(stopping.IsCompleted);
        lease.Dispose();
        NodeLifecycleException failure = await Assert.ThrowsAsync<NodeLifecycleException>(() => stopping);
        Assert.Equal(408, failure.StatusCode);
        Assert.Equal("Stopped", (await host.StatusAsync(node.Node.NodeId)).Availability);
        Assert.True(node.Lifetime.Token.IsCancellationRequested);
        await host.ControlAsync(node.Node.NodeId, "start");
        Assert.NotSame(node.Node, host.Instance(node.Node.NodeId).Node);
    }

    [Fact]
    public async Task StopTwoFollowers_ShouldPreventSuccessfulWritesWithoutChangingMembership()
    {
        await using var host = await ClusterHost.StartAsync();
        NodeStatusResponse leader = await host.WaitForLeaderAsync();
        foreach (RaftKeyValueNode follower in host.Instances.Where(node => node.Node.NodeId != leader.NodeId))
            await host.ControlAsync(follower.Node.NodeId, "stop");
        using var failed = await host.Client.PutAsJsonAsync("/admin/kv/unconfirmed", new PutValueRequest("value"));
        var problem = await AssertProblemAsync(failed, HttpStatusCode.GatewayTimeout, "confirmation_timeout");
        Assert.Equal("unknown", problem.GetProperty("outcome").GetString());
        RaftKeyValueNode remaining = host.Instance(leader.NodeId);
        Assert.Equal(3, remaining.Node.Cluster.ClusterSize);
        Assert.Equal(2, remaining.Node.Cluster.QuorumSize);
        Assert.Equal(1, remaining.Node.LastLogIndex);
        Assert.Equal(0, remaining.Node.CommitIndex);
        Assert.Equal(0, remaining.Node.LastApplied);
        Assert.Null(remaining.Store.Get("unconfirmed"));
    }

    [Fact]
    public async Task StopLeaderDuringSubmission_ShouldDrainWriteAndExcludeStaleLeaderFromRouting()
    {
        await using var host = await ClusterHost.StartAsync();
        NodeStatusResponse leader = await host.WaitForLeaderAsync();
        foreach (RaftKeyValueNode follower in host.Instances.Where(node => node.Node.NodeId != leader.NodeId))
            await host.ControlAsync(follower.Node.NodeId, "stop");
        Task<HttpResponseMessage> writing = host.Client.PutAsJsonAsync(
            "/admin/kv/pending", new PutValueRequest("value"));
        await host.WaitUntilAsync(() => Task.FromResult(host.Instance(leader.NodeId).Node.LastLogIndex == 1));
        await host.ControlAsync(leader.NodeId, "stop");
        using var interrupted = await writing;
        var problem = await AssertProblemAsync(interrupted, HttpStatusCode.Conflict, "leader_stopped");
        Assert.Equal("unknown", problem.GetProperty("outcome").GetString());
        using var rejected = await host.Client.PutAsJsonAsync("/admin/kv/other", new PutValueRequest("value"));
        await AssertProblemAsync(rejected, HttpStatusCode.ServiceUnavailable, "leader_unavailable");
        Assert.Equal(1, host.Instance(leader.NodeId).Node.LastLogIndex);
        Assert.Null(host.Instance(leader.NodeId).Store.Get("pending"));
    }

    [Fact]
    public async Task FailedStart_ShouldRemainUnavailableAndPermitRepairWithoutAffectingOtherNodes()
    {
        await using var host = await ClusterHost.StartAsync();
        NodeStatusResponse leader = await host.WaitForLeaderAsync();
        await host.PutAsync("before", "Alice");
        await host.WaitForAppliedAsync(1);
        string id = host.Instances.First(node => node.Node.NodeId != leader.NodeId).Node.NodeId;
        await host.ControlAsync(id, "stop");
        byte[] validLog = await File.ReadAllBytesAsync(host.LogPath(id));
        await File.WriteAllTextAsync(host.LogPath(id), "invalid persisted log");
        using var failed = await host.Client.PostAsync($"/admin/nodes/{id}/start", null);
        await AssertProblemAsync(failed, HttpStatusCode.InternalServerError, "node_transition_failed");
        Assert.DoesNotContain(host.DirectoryPath, await failed.Content.ReadAsStringAsync());
        Assert.Equal("Faulted", (await host.StatusAsync(id)).Availability);
        Assert.False(host.Instance(id).Lifetime!.IsAvailable);
        await host.PutAsync("during-failed-start", "Bob");
        await File.WriteAllBytesAsync(host.LogPath(id), validLog);
        await host.ControlAsync(id, "start");
        await host.WaitForAppliedAsync(2);
        Assert.Equal("Alice", Text(host.Instance(id), "before"));
        Assert.Equal("Bob", Text(host.Instance(id), "during-failed-start"));
    }

    [Fact]
    public async Task ShutdownDuringTransition_ShouldJoinStopAndRejectNewStarts()
    {
        await using var host = await ClusterHost.StartAsync();
        await host.WaitForLeaderAsync();
        RaftKeyValueNode node = host.Instances[0];
        using IDisposable lease = node.Lifetime!.TryAcquire()!;
        Task<NodeStatusResponse> stopping = host.Manager.StopNodeAsync(node.Node.NodeId, CancellationToken.None);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Task shutdown = host.App.StopAsync(timeout.Token);
        lease.Dispose();
        NodeLifecycleException failure = await Assert.ThrowsAsync<NodeLifecycleException>(() => stopping);
        Assert.Equal(503, failure.StatusCode);
        await shutdown;
        ClusterResponse cluster = await host.Manager.GetClusterAsync(timeout.Token);
        Assert.All(cluster.Nodes, item => Assert.Equal("Stopped", item.Availability));
        NodeLifecycleException rejected = await Assert.ThrowsAsync<NodeLifecycleException>(
            () => host.Manager.StartNodeAsync(node.Node.NodeId, timeout.Token));
        Assert.Equal(503, rejected.StatusCode);
        Assert.All(host.Instances, instance => Assert.False(instance.Lifetime!.IsAvailable));
    }

    [Theory]
    [InlineData("stop")]
    [InlineData("start")]
    [InlineData("restart")]
    public async Task Controls_ShouldRejectUnknownNodeAndNonLoopbackClients(string action)
    {
        await using (var host = await ClusterHost.StartAsync())
        {
            using var unknown = await host.Client.PostAsync($"/admin/nodes/unknown/{action}", null);
            await AssertProblemAsync(unknown, HttpStatusCode.NotFound, "node_not_found");
            Assert.Equal(3, host.Instances.Count);
        }
        await using var remote = await ClusterHost.StartAsync(remoteClient: true);
        using var denied = await remote.Client.PostAsync($"/admin/nodes/node-1/{action}", null);
        Assert.Equal(HttpStatusCode.Forbidden, denied.StatusCode);
        Assert.Equal("application/problem+json", denied.Content.Headers.ContentType?.MediaType);
        Assert.All(remote.Instances, node => Assert.True(node.Lifetime!.IsAvailable));
    }

    [Theory]
    [InlineData("stop")]
    [InlineData("start")]
    [InlineData("restart")]
    public async Task Production_ShouldNotExposeLifecycleRoutes(string action)
    {
        await using var host = await ClusterHost.StartAsync(environment: "Production", enabled: false);
        using var response = await host.Client.PostAsync($"/admin/nodes/node-1/{action}", null);
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    private static string? Text(RaftKeyValueNode node, string key) =>
        node.Store.Get(key) is { } value ? Encoding.UTF8.GetString(value) : null;

    private static async Task<JsonElement> AssertProblemAsync(HttpResponseMessage response, HttpStatusCode status, string code)
    {
        Assert.Equal(status, response.StatusCode);
        Assert.Equal("application/problem+json", response.Content.Headers.ContentType?.MediaType);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(code, problem.GetProperty("title").GetString());
        return problem;
    }

    private sealed class ClusterHost(WebApplication app, HttpClient client, string directory) : IAsyncDisposable
    {
        public WebApplication App { get; } = app;
        public HttpClient Client { get; } = client;
        public string DirectoryPath { get; } = directory;
        public LocalRaftCluster Manager => App.Services.GetRequiredService<LocalRaftCluster>();
        public IReadOnlyList<RaftKeyValueNode> Instances => Manager.GetNodeInstances();
        public RaftKeyValueNode Instance(string id) => Instances.Single(node => node.Node.NodeId == id);
        public string LogPath(string id) => Path.Combine(DirectoryPath, "raft", $"{id}-log.json");
        public string StatePath(string id) => Path.Combine(DirectoryPath, "raft", $"{id}-state.json");
        public async Task<NodeStatusResponse> StatusAsync(string id) =>
            (await Client.GetFromJsonAsync<NodeStatusResponse>($"/admin/nodes/{id}"))!;

        public async Task<NodeStatusResponse> ControlAsync(string id, string action)
        {
            using var response = await Client.PostAsync($"/admin/nodes/{id}/{action}", null);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            NodeStatusResponse status = (await response.Content.ReadFromJsonAsync<NodeStatusResponse>())!;
            Assert.Equal(action == "stop" ? "Stopped" : "Running", status.Availability);
            return status;
        }

        public async Task<DistributedWriteResponse> PutAsync(string key, string value)
        {
            using var response = await Client.PutAsJsonAsync($"/admin/kv/{key}", new PutValueRequest(value));
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            var written = (await response.Content.ReadFromJsonAsync<DistributedWriteResponse>())!;
            Assert.True(written.Committed);
            Assert.True(written.Applied);
            return written;
        }

        public async Task WaitUntilAsync(Func<Task<bool>> condition)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            while (!await condition())
                await Task.Delay(15, timeout.Token);
        }

        public async Task<NodeStatusResponse> WaitForLeaderAsync(string? excluded = null, long afterTerm = 0)
        {
            NodeStatusResponse? leader = null;
            await WaitUntilAsync(async () =>
            {
                var cluster = (await Client.GetFromJsonAsync<ClusterResponse>("/admin/cluster"))!;
                NodeStatusResponse[] leaders = cluster.Nodes.Where(node =>
                    node.Availability == "Running" && node.Role == "Leader").ToArray();
                if (leaders.Length != 1 || leaders[0].NodeId == excluded ||
                    long.Parse(leaders[0].CurrentTerm) <= afterTerm) return false;
                leader = leaders[0];
                return true;
            });
            return leader!;
        }

        public Task WaitForAppliedAsync(long index) => WaitUntilAsync(async () =>
        {
            var cluster = (await Client.GetFromJsonAsync<ClusterResponse>("/admin/cluster"))!;
            return cluster.Nodes.Count == 3 && cluster.Nodes.All(node =>
                node.Availability == "Running" && long.Parse(node.LastApplied) >= index);
        });

        public static async Task<ClusterHost> StartAsync(
            bool remoteClient = false, string environment = "Testing", bool enabled = true)
        {
            string directory = Path.Combine(Path.GetTempPath(), $"aegis-lifecycle-{Guid.NewGuid():N}");
            Directory.CreateDirectory(directory);
            WebApplication? app = null;
            try
            {
                var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = environment });
                builder.WebHost.UseUrls("http://127.0.0.1:0");
                builder.Logging.ClearProviders();
                builder.Configuration["Raft:Enabled"] = enabled.ToString();
                builder.Configuration["Raft:DataDirectory"] = Path.Combine(directory, "raft");
                builder.Services.AddSingleton<KeyValueStore>();
                builder.Services.AddSingleton(new WriteAheadLog(Path.Combine(directory, "legacy.wal")));
                builder.Services.AddSingleton(new SnapshotStore(Path.Combine(directory, "legacy.snapshot")));
                builder.Services.AddSingleton<DatabaseEngine>();
                builder.Services.AddSingleton<LocalRaftCluster>();
                builder.Services.AddSingleton<IClusterMonitor>(services => services.GetRequiredService<LocalRaftCluster>());
                builder.Services.AddSingleton<IDistributedKeyValueService>(services => services.GetRequiredService<LocalRaftCluster>());
                builder.Services.AddSingleton<INodeLifecycleManager>(services => services.GetRequiredService<LocalRaftCluster>());
                builder.Services.AddHostedService(services => services.GetRequiredService<LocalRaftCluster>());
                app = builder.Build();
                if (remoteClient)
                    app.Use((context, next) =>
                    {
                        context.Connection.RemoteIpAddress = IPAddress.Parse("203.0.113.1");
                        return next(context);
                    });
                app.MapAdminEndpoints();
                await app.StartAsync();
                string address = app.Services.GetRequiredService<IServer>().Features
                    .Get<IServerAddressesFeature>()!.Addresses.Single();
                return new ClusterHost(app, new HttpClient
                {
                    BaseAddress = new Uri(address), Timeout = TimeSpan.FromSeconds(15)
                }, directory);
            }
            catch
            {
                if (app is not null) await app.DisposeAsync();
                Directory.Delete(directory, recursive: true);
                throw;
            }
        }

        public async ValueTask DisposeAsync()
        {
            Client.Dispose();
            try
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
                await App.StopAsync(timeout.Token);
            }
            finally
            {
                await App.DisposeAsync();
                Directory.Delete(DirectoryPath, recursive: true);
            }
        }
    }
}
