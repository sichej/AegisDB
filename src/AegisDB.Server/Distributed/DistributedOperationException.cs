namespace AegisDB.Server.Distributed;

/// <summary>
/// Describes a client-operation failure without treating an uncertain write as rolled back.
/// </summary>
public sealed class DistributedOperationException(
    int statusCode,
    string code,
    string message,
    string outcome,
    Exception? innerException = null) : Exception(message, innerException)
{
    public int StatusCode { get; } = statusCode;
    public string Code { get; } = code;
    public string Outcome { get; } = outcome;
}
