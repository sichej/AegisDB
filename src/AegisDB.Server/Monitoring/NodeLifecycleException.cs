namespace AegisDB.Server.Monitoring;

/// <summary>
/// Describes a failed local lifecycle request without exposing persistence details.
/// </summary>
public sealed class NodeLifecycleException(
    int statusCode, string code, string message, Exception? innerException = null)
    : Exception(message, innerException)
{
    public int StatusCode { get; } = statusCode;
    public string Code { get; } = code;
}
