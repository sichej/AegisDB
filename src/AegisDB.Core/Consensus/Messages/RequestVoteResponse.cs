
namespace AegisDB.Core.Consensus.Messages;

/// <summary>
/// Represents the response to a Raft RequestVote RPC.
///
/// The receiving node returns its current term and indicates
/// whether it granted its vote to the candidate.
/// </summary>
public sealed record RequestVoteResponse(
    long Term,
    bool VoteGranted
);
