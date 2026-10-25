
namespace AegisDB.Core.Consensus.Transport;

/// <summary>
/// Represents a recoverable communication failure between Raft nodes.
///
/// This exception must only be used for failures at the transport
/// boundary. State machine and persistence failures must not be
/// converted into transport failures.
/// </summary>
public sealed class RaftTransportException : Exception
{
    public RaftTransportException(string message)
        : base(message)
    {
    }

    public RaftTransportException(
        string message,
        Exception innerException)
        : base(message, innerException)
    {
    }
}
