
namespace AegisDB.Core.Consensus;

/// <summary>
/// Defines the voting members of a Raft cluster.
///
/// Membership is immutable for the lifetime of this configuration.
/// Dynamic membership changes will be implemented separately.
/// </summary>
public sealed class RaftClusterConfiguration
{
    private readonly HashSet<string> _members;

    public int ClusterSize => _members.Count;

    public int QuorumSize => (ClusterSize / 2) + 1;

    public RaftClusterConfiguration(IEnumerable<string> members)
    {
        ArgumentNullException.ThrowIfNull(members);

        var memberList = members.ToArray();

        if (memberList.Length == 0 ||
            memberList.Any(string.IsNullOrWhiteSpace))
        {
            throw new ArgumentException(
                "Cluster must contain valid node identifiers.",
                nameof(members));
        }

        _members = new HashSet<string>(
            memberList,
            StringComparer.Ordinal
        );

        if (_members.Count != memberList.Length)
        {
            throw new ArgumentException(
                "Cluster members must be unique.",
                nameof(members));
        }
    }

    /// <summary>
    /// Determines whether a node belongs to the voting configuration.
    /// </summary>
    public bool Contains(string nodeId)
    {
        return _members.Contains(nodeId);
    }
}
