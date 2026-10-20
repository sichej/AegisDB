
namespace AegisDB.Core.Consensus.Messages;

/// <summary>
/// Represents a Raft RequestVote RPC sent by a candidate
/// to request a vote from another cluster node.
///
/// The receiver uses the candidate's term and last log entry
/// to determine whether the candidate is eligible for its vote.
/// </summary>
public sealed record RequestVoteRequest(
    long Term,
    string CandidateId,
    long LastLogIndex,
    long LastLogTerm
);
