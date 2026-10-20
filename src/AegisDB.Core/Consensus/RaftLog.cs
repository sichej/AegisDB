
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
    private readonly List<RaftLogEntry> _entries = new();
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
}
