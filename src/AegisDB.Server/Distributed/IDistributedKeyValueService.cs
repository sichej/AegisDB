namespace AegisDB.Server.Distributed;

/// <summary>
/// Routes development client operations to Raft-backed state, independently of /kv.
/// </summary>
public interface IDistributedKeyValueService
{
    Task<DistributedWriteResponse> PutAsync(
        string key, string value, CancellationToken cancellationToken);

    Task<DistributedWriteResponse> DeleteAsync(
        string key, CancellationToken cancellationToken);

    Task<DistributedReadResponse> GetAsync(
        string key, CancellationToken cancellationToken);
}
