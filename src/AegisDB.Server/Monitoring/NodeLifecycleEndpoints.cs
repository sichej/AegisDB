namespace AegisDB.Server.Monitoring;

/// <summary>
/// Maps lifecycle controls inside the development-only loopback admin group.
/// </summary>
public static class NodeLifecycleEndpoints
{
    public static void MapNodeLifecycle(this RouteGroupBuilder admin)
    {
        RouteGroupBuilder nodes = admin.MapGroup("/nodes")
            .AddEndpointFilter(async (context, next) =>
            {
                try
                {
                    return await next(context);
                }
                catch (NodeLifecycleException exception)
                {
                    context.HttpContext.RequestServices.GetRequiredService<ILoggerFactory>()
                        .CreateLogger("NodeLifecycle")
                        .LogWarning(exception, "Node lifecycle request failed: {Code}", exception.Code);
                    return TypedResults.Problem(
                        statusCode: exception.StatusCode, title: exception.Code, detail: exception.Message);
                }
            });

        nodes.MapPost("/{id}/stop", async (
            string id, INodeLifecycleManager manager, CancellationToken cancellationToken) =>
            TypedResults.Ok(await manager.StopNodeAsync(id, cancellationToken)))
            .WithName("StopLocalNode")
            .WithSummary("Unregister and drain a local node while preserving its Raft files and membership.");
        nodes.MapPost("/{id}/start", async (
            string id, INodeLifecycleManager manager, CancellationToken cancellationToken) =>
            TypedResults.Ok(await manager.StartNodeAsync(id, cancellationToken)))
            .WithName("StartLocalNode")
            .WithSummary("Start a fresh local node instance from its persisted Raft term, vote, and log.");
        nodes.MapPost("/{id}/restart", async (
            string id, INodeLifecycleManager manager, CancellationToken cancellationToken) =>
            TypedResults.Ok(await manager.RestartNodeAsync(id, cancellationToken)))
            .WithName("RestartLocalNode")
            .WithSummary("Drain a local node and start a fresh instance without changing voting membership.");
    }
}
