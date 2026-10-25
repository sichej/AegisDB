namespace AegisDB.Server.Monitoring;

/// <summary>
/// Provides read-only observations of the locally hosted Raft cluster.
/// </summary>
public interface IClusterMonitor
{
    Task<ClusterResponse> GetClusterAsync(CancellationToken cancellationToken);
}
