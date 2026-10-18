namespace AegisDB.Core.Persistence;

/// <summary>
/// Represents a single operation recorded in the Write-Ahead Log.
/// WAL records are used to reconstruct the database state after a restart.
/// </summary>
public sealed record WalRecord(
    WalOperation Operation,
    string Key,
    byte[]? Value
);