namespace AegisDB.Core.Persistence;

/// <summary>
/// Represents a consistent point-in-time view of the database.
///
/// WalOffset identifies the WAL position immediately after
/// the last mutation included in this snapshot.
///
/// During recovery, only WAL records after this offset
/// should be replayed.
/// </summary>
public sealed record Snapshot(
    long WalOffset,
    IReadOnlyDictionary<string, byte[]> Data
);