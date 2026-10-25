using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using AegisDB.Core;
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Messages;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Consensus.Transport;
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

public sealed class DistributedKeyValueTests
{
    [Fact]
    public async Task PutGetDelete_ShouldUseRealQuorumAndIsolateLegacyStorage()
    {
        await using var host = await ClusterHost.StartAsync();
        host.Transport.Inner.Unregister("node-3");
        DatabaseEngine legacy = host.App.Services.GetRequiredService<DatabaseEngine>();
        await legacy.PutAsync("user:1", Encoding.UTF8.GetBytes("legacy"));

        using var put = await host.Client.PutAsJsonAsync(
            "/admin/kv/user:1", new PutValueRequest("{\"name\":\"Alice\"}"));
        Assert.Equal(HttpStatusCode.OK, put.StatusCode);
        Assert.True(put.Headers.CacheControl?.NoStore);
        var written = (await put.Content.ReadFromJsonAsync<DistributedWriteResponse>())!;
        Assert.Equal("PUT", written.Operation);
        Assert.Equal("node-1", written.LeaderId);
        Assert.Equal("1", written.LogIndex);
        Assert.Equal("1", written.Term);
        Assert.True(written.Committed);
        Assert.True(written.Applied);
        Assert.Equal(1, host.Nodes[0].Node.CommitIndex);
        Assert.Equal(1, host.Nodes[0].Node.LastApplied);
        var persisted = await new RaftLogStore(host.LogPath("node-2")).LoadAsync();
        Assert.Equal("{\"name\":\"Alice\"}", Encoding.UTF8.GetString(
            Assert.IsType<byte[]>(Assert.Single(persisted).Command.Value)));

        var read = (await host.Client.GetFromJsonAsync<DistributedReadResponse>("/admin/kv/user:1"))!;
        Assert.Equal("{\"name\":\"Alice\"}", read.Value);
        Assert.Equal("leader-local-applied", read.Consistency);
        Assert.False(read.Linearizable);
        Assert.Equal("1", read.LastApplied);
        Assert.Equal("legacy", Encoding.UTF8.GetString(legacy.Get("user:1")!));

        using var delete = await host.Client.DeleteAsync("/admin/kv/user:1");
        var deleted = (await delete.Content.ReadFromJsonAsync<DistributedWriteResponse>())!;
        Assert.Equal(HttpStatusCode.OK, delete.StatusCode);
        Assert.Equal("DELETE", deleted.Operation);
        Assert.Equal("2", deleted.LogIndex);
        Assert.True(deleted.Applied);
        using var missing = await host.Client.GetAsync("/admin/kv/user:1");
        Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode);
        var problem = await missing.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("leader-local-applied", problem.GetProperty("consistency").GetString());
        Assert.False(problem.GetProperty("linearizable").GetBoolean());
        Assert.Equal("legacy", Encoding.UTF8.GetString(legacy.Get("user:1")!));
        Assert.True(await legacy.DeleteAsync("user:1"));
        Assert.Null(legacy.Get("user:1"));
    }

    [Fact]
    public async Task EmptyValueAndDeletingMissingKey_ShouldBeCommittedCommands()
    {
        await using var host = await ClusterHost.StartAsync();
        using var put = await host.Client.PutAsJsonAsync("/admin/kv/empty", new PutValueRequest(""));
        Assert.Equal(HttpStatusCode.OK, put.StatusCode);
        Assert.Equal("", (await host.Client.GetFromJsonAsync<DistributedReadResponse>("/admin/kv/empty"))!.Value);
        using var delete = await host.Client.DeleteAsync("/admin/kv/missing");
        Assert.Equal(HttpStatusCode.OK, delete.StatusCode);
        Assert.Equal("2", (await delete.Content.ReadFromJsonAsync<DistributedWriteResponse>())!.LogIndex);
    }

    [Fact]
    public async Task NoQuorum_ShouldTimeOutWithoutRollbackAndMayCommitLater()
    {
        await using var host = await ClusterHost.StartAsync(timeout: TimeSpan.FromMilliseconds(250));
        host.Transport.Inner.Unregister("node-2");
        host.Transport.Inner.Unregister("node-3");
        using var response = await host.Client.PutAsJsonAsync("/admin/kv/pending", new PutValueRequest("value"));
        await AssertProblemAsync(response, HttpStatusCode.GatewayTimeout, "confirmation_timeout", "unknown");
        RaftNode leader = host.Nodes[0].Node;
        Assert.Equal(1, leader.LastLogIndex);
        Assert.Equal(0, leader.CommitIndex);
        Assert.Equal(0, leader.LastApplied);
        Assert.Null(host.Nodes[0].Store.Get("pending"));

        host.Transport.Inner.Register(host.Nodes[1].Node);
        var manager = new RaftReplicationManager(leader, host.Transport, ["node-2", "node-3"]);
        using var recoveryTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        do
        {
            await manager.ReplicateAllOnceAsync(recoveryTimeout.Token);
            await manager.CommitAndApplyAsync(recoveryTimeout.Token);
        } while (leader.CommitIndex < 1);
        Assert.Equal(1, leader.LastLogIndex);
        Assert.Equal(1, leader.CommitIndex);
        Assert.Equal(1, leader.LastApplied);
        Assert.Equal("value", Encoding.UTF8.GetString(host.Nodes[0].Store.Get("pending")!));
    }

    [Theory]
    [InlineData("PUT")]
    [InlineData("DELETE")]
    public async Task NoLeader_ShouldRejectWithoutAppending(string method)
    {
        await using var host = await ClusterHost.StartAsync(elect: false);
        using var request = new HttpRequestMessage(new HttpMethod(method), "/admin/kv/key");
        if (method == "PUT") request.Content = JsonContent.Create(new PutValueRequest("value"));
        using var response = await host.Client.SendAsync(request);
        await AssertProblemAsync(response, HttpStatusCode.ServiceUnavailable, "leader_unavailable", "not-submitted");
        Assert.All(host.Nodes, node => Assert.Equal(0, node.Node.LastLogIndex));
    }

    [Fact]
    public async Task HigherTermResponse_ShouldRejectLeadershipChangeWithoutResubmission()
    {
        await using var host = await ClusterHost.StartAsync();
        host.Transport.Append = (_, _, _) => Task.FromResult(new AppendEntriesResponse(2, false));
        using var response = await host.Client.PutAsJsonAsync("/admin/kv/key", new PutValueRequest("value"));
        await AssertProblemAsync(response, HttpStatusCode.Conflict, "leadership_changed", "unknown");
        Assert.Equal(RaftNodeState.Follower, host.Nodes[0].Node.State);
        Assert.Equal(2, host.Nodes[0].Node.CurrentTerm);
        Assert.Equal(1, host.Nodes[0].Node.LastLogIndex);
        Assert.Equal(0, host.Nodes[0].Node.CommitIndex);
        Assert.Null(host.Nodes[0].Store.Get("key"));
        Assert.All(host.Nodes.Skip(1), node => Assert.Equal(0, node.Node.LastLogIndex));
    }

    [Fact]
    public async Task CancellationDuringReplication_ShouldNotClaimRollback()
    {
        await using var host = await ClusterHost.StartAsync();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        host.Transport.Append = async (_, _, token) =>
        {
            entered.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, token);
            throw new InvalidOperationException("Unreachable after cancellation.");
        };
        using var cancellation = new CancellationTokenSource();
        Task<DistributedWriteResponse> pending = host.Service.PutAsync("cancelled", "value", cancellation.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        cancellation.Cancel();
        var failure = await Assert.ThrowsAsync<DistributedOperationException>(() => pending);
        Assert.Equal(408, failure.StatusCode);
        Assert.Equal("unknown", failure.Outcome);
        Assert.Contains("not rollback", failure.Message);
        Assert.Equal(1, host.Nodes[0].Node.LastLogIndex);
        Assert.Equal(0, host.Nodes[0].Node.CommitIndex);
    }

    [Fact]
    public async Task PersistenceFailure_ShouldReturnProblemWithoutLeakingInternalDetails()
    {
        await using var host = await ClusterHost.StartAsync();
        Directory.CreateDirectory(host.LogPath("node-1"));
        using var response = await host.Client.PutAsJsonAsync("/admin/kv/key", new PutValueRequest("value"));
        await AssertProblemAsync(response, HttpStatusCode.InternalServerError, "distributed_operation_failed", "unknown");
        Assert.DoesNotContain(host.DirectoryPath, await response.Content.ReadAsStringAsync());
        Assert.Equal(0, host.Nodes[0].Node.LastLogIndex);
        Assert.Null(host.Nodes[0].Store.Get("key"));
    }

    [Fact]
    public async Task InvalidInput_ShouldNotAppend()
    {
        await using var host = await ClusterHost.StartAsync();
        foreach (var request in new[]
        {
            (Key: "key", Value: (string?)null),
            (Key: "key", Value: (string?)new string('x', RaftKeyValueService.MaxValueBytes + 1)),
            (Key: new string('k', RaftKeyValueService.MaxKeyBytes + 1), Value: (string?)"value"),
            (Key: " ", Value: (string?)"value")
        })
        {
            using var response = await host.Client.PutAsJsonAsync(
                $"/admin/kv/{Uri.EscapeDataString(request.Key)}", new PutValueRequest(request.Value));
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        }
        Assert.All(host.Nodes, node => Assert.Equal(0, node.Node.LastLogIndex));
    }

    [Theory]
    [InlineData("GET")]
    [InlineData("PUT")]
    [InlineData("DELETE")]
    public async Task Production_ShouldNotMapAdminOperations(string method)
    {
        await using var host = await ClusterHost.StartAsync(environment: "Production");
        using var request = new HttpRequestMessage(new HttpMethod(method), "/admin/kv/key");
        if (method == "PUT") request.Content = JsonContent.Create(new PutValueRequest("value"));
        using var response = await host.Client.SendAsync(request);
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.All(host.Nodes, node => Assert.Equal(0, node.Node.LastLogIndex));
    }

    private static async Task AssertProblemAsync(
        HttpResponseMessage response, HttpStatusCode status, string title, string outcome)
    {
        Assert.Equal(status, response.StatusCode);
        Assert.Equal("application/problem+json", response.Content.Headers.ContentType?.MediaType);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(title, problem.GetProperty("title").GetString());
        Assert.Equal(outcome, problem.GetProperty("outcome").GetString());
        Assert.False(problem.GetProperty("automaticRetry").GetBoolean());
    }

    private sealed class ControlledTransport : IRaftTransport
    {
        public InMemoryRaftTransport Inner { get; } = new();
        public Func<string, AppendEntriesRequest, CancellationToken, Task<AppendEntriesResponse>>? Append { get; set; }
        public Task<RequestVoteResponse> RequestVoteAsync(
            string targetNodeId, RequestVoteRequest request, CancellationToken cancellationToken = default) =>
            Inner.RequestVoteAsync(targetNodeId, request, cancellationToken);
        public Task<AppendEntriesResponse> AppendEntriesAsync(
            string targetNodeId, AppendEntriesRequest request, CancellationToken cancellationToken = default) =>
            Append is null ? Inner.AppendEntriesAsync(targetNodeId, request, cancellationToken)
                : Append(targetNodeId, request, cancellationToken);
    }

    private sealed class ClusterHost(
        WebApplication app, HttpClient client, string directory,
        RaftKeyValueNode[] nodes, ControlledTransport transport, RaftKeyValueService service) : IAsyncDisposable
    {
        public WebApplication App { get; } = app;
        public HttpClient Client { get; } = client;
        public string DirectoryPath { get; } = directory;
        public RaftKeyValueNode[] Nodes { get; } = nodes;
        public ControlledTransport Transport { get; } = transport;
        public RaftKeyValueService Service { get; } = service;
        public string LogPath(string id) => Path.Combine(DirectoryPath, $"{id}-log.json");

        public static async Task<ClusterHost> StartAsync(
            bool elect = true, TimeSpan? timeout = null, string environment = "Testing")
        {
            string directory = Path.Combine(Path.GetTempPath(), $"aegis-explorer-{Guid.NewGuid():N}");
            Directory.CreateDirectory(directory);
            WebApplication? app = null;
            try
            {
                string[] members = ["node-1", "node-2", "node-3"];
                var cluster = new RaftClusterConfiguration(members);
                var transport = new ControlledTransport();
                var nodes = new List<RaftKeyValueNode>();
                foreach (string id in members)
                {
                    var store = new KeyValueStore();
                    var node = await RaftNode.CreateAsync(id, cluster,
                        new RaftStateStore(Path.Combine(directory, $"{id}-state.json")),
                        new RaftLogStore(Path.Combine(directory, $"{id}-log.json")),
                        stateMachine: new RaftStateMachine(store));
                    nodes.Add(new RaftKeyValueNode(node, store));
                    transport.Inner.Register(node);
                }
                if (elect)
                {
                    Assert.True(await new RaftElectionCoordinator(
                        nodes[0].Node, transport, members.Skip(1)).RunElectionAsync());
                }
                var service = new RaftKeyValueService(nodes, transport, timeout);
                var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = environment });
                builder.WebHost.UseUrls("http://127.0.0.1:0");
                builder.Logging.ClearProviders();
                builder.Services.AddSingleton<IDistributedKeyValueService>(service);
                builder.Services.AddSingleton<LocalRaftCluster>();
                builder.Services.AddSingleton<IClusterMonitor>(services => services.GetRequiredService<LocalRaftCluster>());
                builder.Services.AddSingleton<KeyValueStore>();
                builder.Services.AddSingleton(new WriteAheadLog(Path.Combine(directory, "legacy.wal")));
                builder.Services.AddSingleton(new SnapshotStore(Path.Combine(directory, "legacy.snapshot")));
                builder.Services.AddSingleton<DatabaseEngine>();
                app = builder.Build();
                app.MapAdminEndpoints();
                await app.StartAsync();
                string address = app.Services.GetRequiredService<IServer>()
                    .Features.Get<IServerAddressesFeature>()!.Addresses.Single();
                return new ClusterHost(app, new HttpClient
                {
                    BaseAddress = new Uri(address),
                    Timeout = TimeSpan.FromSeconds(10)
                }, directory, nodes.ToArray(), transport, service);
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
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
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
