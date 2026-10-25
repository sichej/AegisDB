using System.Net;
using AegisDB.Core;
using AegisDB.Core.Persistence;
using AegisDB.Server.Distributed;
using Microsoft.AspNetCore.Http.HttpResults;

namespace AegisDB.Server.Monitoring;

/// <summary>
/// Maps development-only loopback monitoring, local node controls, and snapshot operations.
/// </summary>
public static class AdminEndpoints
{
    public static void MapAdminEndpoints(this WebApplication app)
    {
        if (!app.Environment.IsDevelopment() && !app.Environment.IsEnvironment("Testing"))
            return;

        RouteGroupBuilder admin = app.MapGroup("/admin")
            .AddEndpointFilter(async (context, next) =>
            {
                IPAddress? address = context.HttpContext.Connection.RemoteIpAddress;

                if (address is null || !IPAddress.IsLoopback(address))
                {
                    return TypedResults.Problem(
                        statusCode: StatusCodes.Status403Forbidden,
                        title: "Admin endpoints are restricted to loopback clients.");
                }

                context.HttpContext.Response.Headers.CacheControl = "no-store";
                return await next(context);
            });

        admin.MapDistributedKeyValue();
        if (app.Configuration.GetValue<bool>("Raft:Enabled"))
            admin.MapNodeLifecycle();

        admin.MapGet("/cluster", async (
            IClusterMonitor monitor, CancellationToken cancellationToken) =>
            TypedResults.Ok(await monitor.GetClusterAsync(cancellationToken)))
            .WithName("GetCluster")
            .WithSummary("Observe the Raft nodes hosted in this process.");

        admin.MapGet("/nodes", async (
            IClusterMonitor monitor, CancellationToken cancellationToken) =>
            TypedResults.Ok((await monitor.GetClusterAsync(cancellationToken)).Nodes))
            .WithName("GetNodes")
            .WithSummary("List local Raft snapshots and runtime availability.");

        admin.MapGet("/nodes/{id}",
            async Task<Results<Ok<NodeStatusResponse>, ProblemHttpResult>> (
                string id, IClusterMonitor monitor, CancellationToken cancellationToken) =>
            {
                ClusterResponse cluster = await monitor.GetClusterAsync(cancellationToken);
                NodeStatusResponse? node = cluster.Nodes.FirstOrDefault(node => node.NodeId == id);

                return node is not null
                    ? TypedResults.Ok(node)
                    : TypedResults.Problem(
                        statusCode: StatusCodes.Status404NotFound,
                        title: "No locally hosted node has this ID.");
            })
            .WithName("GetNode")
            .WithSummary("Observe one locally hosted Raft node.");

        admin.MapPost("/snapshot", async (
            DatabaseEngine db,
            SnapshotStore snapshotStore,
            CancellationToken cancellationToken) =>
        {
            Snapshot snapshot = await db.CreateSnapshotAsync(cancellationToken);
            await snapshotStore.SaveAsync(snapshot, cancellationToken);

            return TypedResults.Ok(new
            {
                walOffset = snapshot.WalOffset,
                entries = snapshot.Data.Count
            });
        })
            .WithSummary("Persist a snapshot of the independent single-node key-value engine.");
    }
}
