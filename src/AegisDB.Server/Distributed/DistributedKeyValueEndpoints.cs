using Microsoft.AspNetCore.Http.HttpResults;

namespace AegisDB.Server.Distributed;

/// <summary>
/// Maps development-only Raft-backed key-value operations inside the protected admin group.
/// </summary>
public static class DistributedKeyValueEndpoints
{
    public static void MapDistributedKeyValue(this RouteGroupBuilder admin)
    {
        RouteGroupBuilder kv = admin.MapGroup("/kv")
            .AddEndpointFilter(async (context, next) =>
            {
                ILogger logger = context.HttpContext.RequestServices
                    .GetRequiredService<ILoggerFactory>().CreateLogger("DistributedKeyValue");
                try
                {
                    return await next(context);
                }
                catch (DistributedOperationException exception)
                {
                    logger.LogWarning(exception, "Distributed operation failed: {Code}", exception.Code);
                    return TypedResults.Problem(
                        statusCode: exception.StatusCode,
                        title: exception.Code,
                        detail: exception.Message,
                        extensions: new Dictionary<string, object?>
                        {
                            ["outcome"] = exception.Outcome,
                            ["automaticRetry"] = false
                        });
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    logger.LogError(exception, "Distributed operation failed internally.");
                    return TypedResults.Problem(
                        statusCode: StatusCodes.Status500InternalServerError,
                        title: "distributed_operation_failed",
                        detail: "Persistence or replication failed. A submitted command may still commit; "
                            + "the request was not automatically retried.",
                        extensions: new Dictionary<string, object?>
                        {
                            ["outcome"] = "unknown",
                            ["automaticRetry"] = false
                        });
                }
            });

        kv.MapPut("/{key}", async Task<Ok<DistributedWriteResponse>> (
            string key,
            PutValueRequest request,
            IDistributedKeyValueService service,
            CancellationToken cancellationToken) =>
        {
            if (request.Value is null)
            {
                throw new DistributedOperationException(
                    400, "invalid_value",
                    "A string value is required; an empty string is allowed.", "not-submitted");
            }

            return TypedResults.Ok(await service.PutAsync(key, request.Value, cancellationToken));
        })
            .WithName("PutDistributedValue")
            .WithSummary("Submit a PUT once and wait for Raft quorum commitment and local application.");

        kv.MapDelete("/{key}", async (
            string key, IDistributedKeyValueService service, CancellationToken cancellationToken) =>
            TypedResults.Ok(await service.DeleteAsync(key, cancellationToken)))
            .WithName("DeleteDistributedValue")
            .WithSummary("Submit an idempotent DELETE command through Raft, including for a missing key.");

        kv.MapGet("/{key}", async Task<Results<Ok<DistributedReadResponse>, ProblemHttpResult>> (
            string key, IDistributedKeyValueService service, CancellationToken cancellationToken) =>
        {
            DistributedReadResponse result = await service.GetAsync(key, cancellationToken);
            return result.Value is not null
                ? TypedResults.Ok(result)
                : TypedResults.Problem(
                    statusCode: StatusCodes.Status404NotFound,
                    title: "key_not_found",
                    detail: "The key was not found in the observed leader's locally applied state.",
                    extensions: new Dictionary<string, object?>
                    {
                        ["consistency"] = result.Consistency,
                        ["linearizable"] = result.Linearizable,
                        ["leaderId"] = result.LeaderId,
                        ["term"] = result.Term,
                        ["lastApplied"] = result.LastApplied
                    });
        })
            .WithName("GetDistributedValue")
            .WithSummary("Read leader-local applied state without a linearizable read barrier.");
    }
}
