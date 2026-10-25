namespace AegisDB.Server.Monitoring;

/// <summary>
/// Controls local node instances without changing Raft voting membership.
/// </summary>
public interface INodeLifecycleManager
{
    Task<NodeStatusResponse> StopNodeAsync(string nodeId, CancellationToken cancellationToken);
    Task<NodeStatusResponse> StartNodeAsync(string nodeId, CancellationToken cancellationToken);
    Task<NodeStatusResponse> RestartNodeAsync(string nodeId, CancellationToken cancellationToken);
}
