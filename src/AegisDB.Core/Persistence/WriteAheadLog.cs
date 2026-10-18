namespace AegisDB.Core.Persistence;

/// <summary>
/// Manages the persistent log of database mutations.
/// Operations must be durably recorded before they are
/// considered committed by the storage engine.
/// </summary>
public sealed class WriteAheadLog
{
    private readonly string _filePath;

    public WriteAheadLog(string filePath)
    {
        _filePath = filePath;
    }
}