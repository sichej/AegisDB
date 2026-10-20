namespace AegisDB.Core.Consensus;

/// <summary>
/// Represents the possible roles of a node in the Raft consensus protocol.
///
/// A node starts as a Follower and may transition to Candidate
/// or Leader depending on elections and messages from other nodes.
/// </summary>
public enum RaftNodeState
{
    Follower,
    Candidate,
    Leader
}