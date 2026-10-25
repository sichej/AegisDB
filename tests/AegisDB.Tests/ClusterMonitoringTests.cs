using System.Net;
using System.Net.Http.Json;
using AegisDB.Core;
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Persistence;
using AegisDB.Core.Storage;
using AegisDB.Server.Monitoring;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AegisDB.Tests;

public sealed class ClusterMonitoringTests
{
    [Fact]
    public async Task Endpoints_ShouldObserveRealRunningNodesAndPersistedLogs()
    {
        await using var host = await MonitoringHost.StartAsync(enabled: true);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        ClusterResponse cluster;
        do
        {
            cluster = (await host.Client.GetFromJsonAsync<ClusterResponse>(
                "/admin/cluster", timeout.Token))!;

            if (!cluster.Nodes.Any(node => node.Role == "Leader"))
                await Task.Delay(20, timeout.Token);
        }
        while (!cluster.Nodes.Any(node => node.Role == "Leader"));

        Assert.True(cluster.Enabled);
        Assert.Equal("local-in-process", cluster.Mode);
        Assert.Contains("not network reachability", cluster.AvailabilityDescription);
        Assert.InRange(cluster.ObservedAt, DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow);
        Assert.Equal(new[] { "node-1", "node-2", "node-3" }, cluster.Nodes.Select(node => node.NodeId));
        Assert.Single(cluster.Nodes, node => node.Role == "Leader");

        foreach (NodeStatusResponse node in cluster.Nodes)
        {
            Assert.Equal("Running", node.Availability);
            Assert.Contains(node.Role, new[] { "Follower", "Candidate", "Leader" });
            Assert.True(long.Parse(node.CurrentTerm) >= 5);
            Assert.Equal("0", node.CommitIndex);
            Assert.Equal("0", node.LastApplied);
            Assert.Equal("2", node.LastLogIndex);
            Assert.Equal("5", node.LastLogTerm);

            using var response = await host.Client.GetAsync($"/admin/nodes/{node.NodeId}", timeout.Token);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.True(response.Headers.CacheControl?.NoStore);
            NodeStatusResponse details = (await response.Content.ReadFromJsonAsync<NodeStatusResponse>(
                timeout.Token))!;
            Assert.Equal(node.NodeId, details.NodeId);
            Assert.Equal(node.LastLogIndex, details.LastLogIndex);
            Assert.Equal(node.LastLogTerm, details.LastLogTerm);
            Assert.Equal("Running", details.Availability);
        }

        NodeStatusResponse[] nodes = (await host.Client.GetFromJsonAsync<NodeStatusResponse[]>(
            "/admin/nodes", timeout.Token))!;
        Assert.Equal(cluster.Nodes.Select(node => node.NodeId), nodes.Select(node => node.NodeId));
    }

    [Fact]
    public async Task DisabledCluster_ShouldNotFabricateNodesOrCreateRaftStorage()
    {
        await using var host = await MonitoringHost.StartAsync(enabled: false);
        ClusterResponse cluster = (await host.Client.GetFromJsonAsync<ClusterResponse>("/admin/cluster"))!;
        Assert.False(cluster.Enabled);
        Assert.Equal("disabled", cluster.Mode);
        Assert.Empty(cluster.Nodes);
        Assert.False(Directory.Exists(host.RaftDirectory));
        Assert.Empty((await host.Client.GetFromJsonAsync<NodeStatusResponse[]>("/admin/nodes"))!);
    }

    [Fact]
    public async Task UnknownNode_ShouldReturnNotFoundProblem()
    {
        await using var host = await MonitoringHost.StartAsync(enabled: false);
        using var response = await host.Client.GetAsync("/admin/nodes/unknown");
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Equal("application/problem+json", response.Content.Headers.ContentType?.MediaType);
    }

    [Theory]
    [InlineData("GET", "/admin/cluster")]
    [InlineData("GET", "/admin/nodes")]
    [InlineData("GET", "/admin/nodes/node-1")]
    [InlineData("POST", "/admin/snapshot")]
    public async Task AdminEndpoints_ShouldRejectNonLoopbackClients(string method, string path)
    {
        await using var host = await MonitoringHost.StartAsync(enabled: false, remoteClient: true);
        using var request = new HttpRequestMessage(new HttpMethod(method), path);
        using var response = await host.Client.SendAsync(request);
        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal("application/problem+json", response.Content.Headers.ContentType?.MediaType);
    }

    [Theory]
    [InlineData("/admin/nodes/node-1/stop")]
    [InlineData("/admin/nodes/node-1/restart")]
    public async Task Monitoring_ShouldNotExposeNodeMutationControls(string path)
    {
        await using var host = await MonitoringHost.StartAsync(enabled: false);
        using var response = await host.Client.PostAsync(path, null);
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task Cluster_ShouldRejectProductionHosting()
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            EnvironmentName = Environments.Production
        });
        builder.Configuration["Raft:Enabled"] = "true";
        await using WebApplication app = builder.Build();
        using var cluster = new LocalRaftCluster(
            app.Configuration, app.Environment, app.Services.GetRequiredService<ILogger<LocalRaftCluster>>());
        await Assert.ThrowsAsync<InvalidOperationException>(() => cluster.StartAsync(CancellationToken.None));
    }

    [Fact]
    public async Task Cluster_ShouldStopRuntimesBeforeShutdownCompletes()
    {
        await using var host = await MonitoringHost.StartAsync(enabled: true);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await host.App.StopAsync(timeout.Token);
        ClusterResponse cluster = await host.App.Services.GetRequiredService<IClusterMonitor>()
            .GetClusterAsync(timeout.Token);
        Assert.All(cluster.Nodes, node => Assert.Equal("Stopped", node.Availability));

        foreach (string nodeId in cluster.Nodes.Select(node => node.NodeId))
        {
            using var stream = new FileStream(
                Path.Combine(host.RaftDirectory, $"{nodeId}-log.json"),
                FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            Assert.True(stream.Length > 0);
        }
    }

    private sealed class MonitoringHost(WebApplication app, HttpClient client, string directory)
        : IAsyncDisposable
    {
        public WebApplication App { get; } = app;
        public HttpClient Client { get; } = client;
        public string RaftDirectory => Path.Combine(directory, "raft");

        public static async Task<MonitoringHost> StartAsync(bool enabled, bool remoteClient = false)
        {
            string directory = Path.Combine(Path.GetTempPath(), $"aegis-monitor-{Guid.NewGuid():N}");
            Directory.CreateDirectory(directory);
            WebApplication? app = null;

            try
            {
                if (enabled)
                {
                    foreach (string nodeId in new[] { "node-1", "node-2", "node-3" })
                    {
                        await new RaftStateStore(Path.Combine(directory, "raft", $"{nodeId}-state.json"))
                            .SaveAsync(new RaftPersistentState(5, null));
                        await new RaftLogStore(Path.Combine(directory, "raft", $"{nodeId}-log.json"))
                            .SaveAsync([
                                new RaftLogEntry(1, 5, new WalRecord(WalOperation.Put, "first", [1])),
                                new RaftLogEntry(2, 5, new WalRecord(WalOperation.Delete, "first", null))
                            ]);
                    }
                }

                var builder = WebApplication.CreateBuilder(new WebApplicationOptions
                {
                    EnvironmentName = "Testing"
                });
                builder.WebHost.UseUrls("http://127.0.0.1:0");
                builder.Logging.ClearProviders();
                builder.Configuration["Raft:Enabled"] = enabled.ToString();
                builder.Configuration["Raft:DataDirectory"] = Path.Combine(directory, "raft");
                builder.Services.AddSingleton<KeyValueStore>();
                builder.Services.AddSingleton(new WriteAheadLog(Path.Combine(directory, "test.wal")));
                builder.Services.AddSingleton(new SnapshotStore(Path.Combine(directory, "test.snapshot")));
                builder.Services.AddSingleton<DatabaseEngine>();
                builder.Services.AddSingleton<LocalRaftCluster>();
                builder.Services.AddSingleton<IClusterMonitor>(services =>
                    services.GetRequiredService<LocalRaftCluster>());
                builder.Services.AddHostedService(services =>
                    services.GetRequiredService<LocalRaftCluster>());

                app = builder.Build();
                if (remoteClient)
                {
                    app.Use((context, next) =>
                    {
                        context.Connection.RemoteIpAddress = IPAddress.Parse("203.0.113.1");
                        return next(context);
                    });
                }

                app.MapAdminEndpoints();
                await app.StartAsync();
                string address = app.Services.GetRequiredService<IServer>()
                    .Features.Get<IServerAddressesFeature>()!.Addresses.Single();
                var client = new HttpClient
                {
                    BaseAddress = new Uri(address),
                    Timeout = TimeSpan.FromSeconds(5)
                };
                return new MonitoringHost(app, client, directory);
            }
            catch
            {
                if (app is not null)
                    await app.DisposeAsync();
                Directory.Delete(directory, recursive: true);
                throw;
            }
        }

        public async ValueTask DisposeAsync()
        {
            Client.Dispose();
            try
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                await App.StopAsync(timeout.Token);
            }
            finally
            {
                await App.DisposeAsync();
                Directory.Delete(directory, recursive: true);
            }
        }
    }
}
