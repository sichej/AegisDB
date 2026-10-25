using System.Globalization;
using System.Text;
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Transport;
using AegisDB.Core.Persistence;
using AegisDB.Core.Storage;

namespace AegisDB.Server.Distributed;

/// <summary>
/// Associates a Raft node with the same store injected into its state machine.
/// </summary>
public sealed record RaftKeyValueNode(RaftNode Node, KeyValueStore Store);

/// <summary>
/// Submits each write once through existing Raft services and observes leader-local applied reads.
/// </summary>
public sealed class RaftKeyValueService : IDistributedKeyValueService
{
    public const int MaxKeyBytes = 1024;
    public const int MaxValueBytes = 1024 * 1024;

    private readonly IReadOnlyList<RaftKeyValueNode> _nodes;
    private readonly IRaftTransport _transport;
    private readonly TimeSpan _timeout;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    public RaftKeyValueService(
        IReadOnlyList<RaftKeyValueNode> nodes,
        IRaftTransport transport,
        TimeSpan? timeout = null)
    {
        _nodes = nodes ?? throw new ArgumentNullException(nameof(nodes));
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _timeout = timeout ?? TimeSpan.FromSeconds(5);
        if (_timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(timeout));
    }

    public Task<DistributedWriteResponse> PutAsync(
        string key, string value, CancellationToken cancellationToken)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(value);
        byte[] bytes = Encoding.UTF8.GetBytes(value);
        if (bytes.Length > MaxValueBytes)
        {
            throw new DistributedOperationException(
                400, "invalid_value", "Values must not exceed 1 MiB of UTF-8 data.", "not-submitted");
        }

        return SubmitAsync(new WalRecord(WalOperation.Put, key, bytes), cancellationToken);
    }

    public Task<DistributedWriteResponse> DeleteAsync(
        string key, CancellationToken cancellationToken)
    {
        ValidateKey(key);
        return SubmitAsync(new WalRecord(WalOperation.Delete, key, null), cancellationToken);
    }

    private async Task<DistributedWriteResponse> SubmitAsync(
        WalRecord command, CancellationToken cancellationToken)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(_timeout);
        bool entered = false;
        bool submissionStarted = false;
        RaftKeyValueNode? leader = null;
        long term = 0;

        try
        {
            await _writeLock.WaitAsync(deadline.Token);
            entered = true;
            (leader, term) = await ResolveLeaderAsync(deadline.Token);

            string[] peers = _nodes
                .Where(node => node.Node.NodeId != leader.Node.NodeId)
                .Select(node => node.Node.NodeId)
                .ToArray();
            var manager = new RaftReplicationManager(leader.Node, _transport, peers);
            var submission = new RaftCommandSubmissionService(leader.Node, manager);

            // Never retry SubmitAsync: its internal retries replicate the one appended entry.
            submissionStarted = true;
            long index = await submission.SubmitAsync(command, deadline.Token);
            RaftNodeStateSnapshot state = await leader.Node.GetStateSnapshotAsync(deadline.Token);
            RaftLogEntry entry = leader.Node.Log.GetEntry(index)
                ?? throw new InvalidOperationException("The confirmed Raft entry is missing.");

            if (state.State != RaftNodeState.Leader ||
                state.CurrentTerm != term || entry.Term != term)
            {
                throw LeadershipChanged();
            }

            if (state.CommitIndex < index || state.LastApplied < index)
                throw new InvalidOperationException("Raft returned an unconfirmed command.");

            return new DistributedWriteResponse(
                command.Key,
                command.Operation.ToString().ToUpperInvariant(),
                state.NodeId,
                index.ToString(CultureInfo.InvariantCulture),
                entry.Term.ToString(CultureInfo.InvariantCulture),
                Committed: true,
                Applied: true);
        }
        catch (OperationCanceledException exception)
        {
            string outcome = submissionStarted ? "unknown" : "not-submitted";
            if (cancellationToken.IsCancellationRequested)
            {
                throw new DistributedOperationException(
                    408, "request_cancelled",
                    "The request was cancelled. An already submitted command may still commit; cancellation is not rollback.",
                    outcome, exception);
            }

            throw new DistributedOperationException(
                504, "confirmation_timeout",
                "Quorum commitment and local application were not confirmed before the deadline. "
                    + "A submitted command may still commit later. No automatic resubmission was performed.",
                outcome, exception);
        }
        catch (InvalidOperationException exception) when (
            leader is not null &&
            (leader.Node.State != RaftNodeState.Leader || leader.Node.CurrentTerm != term))
        {
            throw LeadershipChanged(exception);
        }
        finally
        {
            if (entered)
                _writeLock.Release();
        }
    }

    public async Task<DistributedReadResponse> GetAsync(
        string key, CancellationToken cancellationToken)
    {
        ValidateKey(key);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(_timeout);

        try
        {
            (RaftKeyValueNode leader, long term) = await ResolveLeaderAsync(deadline.Token);
            await leader.Node.ApplyCommittedEntriesAsync(deadline.Token);
            byte[]? value = leader.Store.Get(key);
            RaftNodeStateSnapshot afterRead =
                await leader.Node.GetStateSnapshotAsync(deadline.Token);

            if (afterRead.State != RaftNodeState.Leader || afterRead.CurrentTerm != term)
                throw LeadershipChanged();

            return new DistributedReadResponse(
                key,
                value is null ? null : Encoding.UTF8.GetString(value),
                afterRead.NodeId,
                term.ToString(CultureInfo.InvariantCulture),
                afterRead.LastApplied.ToString(CultureInfo.InvariantCulture),
                "leader-local-applied",
                Linearizable: false);
        }
        catch (OperationCanceledException exception)
        {
            throw new DistributedOperationException(
                cancellationToken.IsCancellationRequested ? 408 : 504,
                cancellationToken.IsCancellationRequested ? "request_cancelled" : "read_timeout",
                "The local read was not completed before cancellation or timeout.",
                "not-applicable", exception);
        }
    }

    private async Task<(RaftKeyValueNode Node, long Term)> ResolveLeaderAsync(
        CancellationToken cancellationToken)
    {
        var observations = new List<(RaftKeyValueNode Node, RaftNodeStateSnapshot State)>();
        foreach (RaftKeyValueNode node in _nodes)
        {
            observations.Add((node, await node.Node.GetStateSnapshotAsync(cancellationToken)));
        }

        long highestTerm = observations.Count == 0 ? 0 : observations.Max(item => item.State.CurrentTerm);
        var leaders = observations.Where(item =>
            item.State.State == RaftNodeState.Leader && item.State.CurrentTerm == highestTerm).ToArray();

        if (leaders.Length != 1)
        {
            throw new DistributedOperationException(
                503, "leader_unavailable",
                "No unique leader is currently known in the highest observed term. No command was submitted.",
                "not-submitted");
        }

        return (leaders[0].Node, highestTerm);
    }

    private static DistributedOperationException LeadershipChanged(Exception? innerException = null) =>
        new(409, "leadership_changed",
            "Leadership changed before confirmation. A submitted command may still commit. "
                + "No automatic resubmission was performed.",
            "unknown", innerException);

    private static void ValidateKey(string key)
    {
        if (string.IsNullOrWhiteSpace(key) || Encoding.UTF8.GetByteCount(key) > MaxKeyBytes)
        {
            throw new DistributedOperationException(
                400, "invalid_key", "Keys must contain 1 to 1024 UTF-8 bytes and not be blank.", "not-submitted");
        }
    }
}
