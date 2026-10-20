
namespace AegisDB.Core.Consensus;

/// <summary>
/// Maintains the ordered sequence of entries in a Raft log.
///
/// Log indices start at 1. Index 0 represents the empty-log
/// boundary used by Raft during consistency checks.
///
/// This implementation is currently in-memory and does not
/// support log truncation or snapshot compaction.
/// </summary>
public sealed class RaftLog
{
    private List<RaftLogEntry> _entries = new();
    private readonly object _lock = new();

    public long LastIndex
    {
        get
        {
            lock (_lock)
            {
                return _entries.Count;
            }
        }
    }

    public long LastTerm
    {
        get
        {
            lock (_lock)
            {
                return _entries.Count == 0
                    ? 0
                    : _entries[^1].Term;
            }
        }
    }

    /// <summary>
    /// Appends an entry only if its index is the next
    /// expected position in the log.
    /// </summary>
    public void Append(RaftLogEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        lock (_lock)
        {
            long expectedIndex = _entries.Count + 1L;

            if (entry.Index != expectedIndex)
            {
                throw new InvalidOperationException(
                    $"Expected log index {expectedIndex}, " +
                    $"but received {entry.Index}.");
            }

            if (entry.Term < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(entry),
                    "Raft term cannot be negative.");
            }

            _entries.Add(entry);
        }
    }

    /// <summary>
    /// Returns the entry at the specified Raft index,
    /// or null if that index does not exist.
    /// </summary>
    public RaftLogEntry? GetEntry(long index)
    {
        lock (_lock)
        {
            if (index < 1 || index > _entries.Count)
                return null;

            return _entries[checked((int)(index - 1))];
        }
    }

    /// <summary>
    /// Checks whether the local log contains an entry
    /// matching the specified index and term.
    ///
    /// Index 0 and term 0 always match the empty-log boundary.
    /// </summary>
    public bool Matches(long index, long term)
    {
        lock (_lock)
        {
            if (index == 0)
                return term == 0;

            if (index < 0 || index > _entries.Count)
                return false;

            return _entries[checked((int)(index - 1))].Term == term;
        }
    }


    /// <summary>
    /// Removes all log entries starting from the specified index,
    /// including the entry at that index.
    ///
    /// Index 1 clears the entire log. LastIndex + 1 is a no-op.
    ///
    /// The caller must ensure that committed entries are never
    /// truncated. Commit index tracking is not implemented yet.
    /// </summary>
    public void TruncateFrom(long index)
    {
        lock (_lock)
        {
            if (index < 1 || index > _entries.Count + 1L)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(index),
                    "Truncation index is outside the valid log range.");
            }

            int startPosition = checked((int)(index - 1));

            _entries.RemoveRange(
                startPosition,
                _entries.Count - startPosition);
        }
    }


    /// <summary>
    /// Reconciles incoming entries with the local Raft log.
    ///
    /// The complete incoming sequence is validated before the
    /// existing log is modified. Matching entries are preserved,
    /// while the first conflicting entry and its suffix are replaced.
    ///
    /// This method provides in-memory validation atomicity.
    /// Durable persistence and committed-entry protection are
    /// separate responsibilities not yet implemented.
    /// </summary>
    public void AppendEntries(IReadOnlyList<RaftLogEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);

        lock (_lock)
        {
            if (entries.Count == 0)
                return;

            // Snapshot the incoming collection to prevent external
            // changes to its structure during reconciliation.
            RaftLogEntry[] incoming = entries.ToArray();

            // Validate the entire sequence before changing the log.
            for (int i = 0; i < incoming.Length; i++)
            {
                RaftLogEntry entry = incoming[i];

                ArgumentNullException.ThrowIfNull(entry);

                if (entry.Index < 1 || entry.Term < 0)
                {
                    throw new ArgumentException(
                        "Invalid Raft log entry.",
                        nameof(entries));
                }

                if (i > 0 &&
                    entry.Index != incoming[i - 1].Index + 1)
                {
                    throw new ArgumentException(
                        "Incoming entries must have consecutive indices.",
                        nameof(entries));
                }
            }

            long firstIndex = incoming[0].Index;

            if (firstIndex > _entries.Count + 1L)
            {
                throw new InvalidOperationException(
                    "Cannot append entries with a missing predecessor.");
            }

            // Construct the replacement log before modifying the
            // current one. If validation or allocation fails,
            // the original log remains unchanged.
            var updated = new List<RaftLogEntry>(_entries);

            foreach (RaftLogEntry entry in incoming)
            {
                int position = checked((int)(entry.Index - 1));

                if (position < updated.Count)
                {
                    if (updated[position].Term == entry.Term)
                        continue;

                    updated.RemoveRange(
                        position,
                        updated.Count - position);
                }

                if (position != updated.Count)
                {
                    throw new InvalidOperationException(
                        "Incoming entries cannot be reconciled.");
                }

                updated.Add(entry);
            }

            // Publish the completed replacement with one reference assignment.
            _entries = updated;
        }
    }


}
