namespace AegisDB.Core.Persistence;

/// <summary>
/// Contains the validated WAL records and the location at which
/// the valid portion of the log ends.
/// </summary>
/// <param name="Records">
/// Records successfully read and validated in append order.
/// </param>
/// <param name="LastValidOffset">
/// Byte offset immediately after the last complete, valid record.
/// </param>
/// <param name="HasIncompleteTail">
/// Indicates that the file ends with an incomplete WAL record.
/// </param>
public sealed record WalReadResult(
    IReadOnlyList<WalRecord> Records,
    long LastValidOffset,
    bool HasIncompleteTail
);