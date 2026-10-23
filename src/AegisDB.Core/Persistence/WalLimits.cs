
namespace AegisDB.Core.Persistence;

/// <summary>
/// Defines shared WAL format limits used by both readers and writers.
/// </summary>
public static class WalLimits
{
    public const int MaxRecordPayloadBytes = 16 * 1024 * 1024;

    public const int RecordOverheadBytes =
        sizeof(byte) +  // Operation
        sizeof(int) +   // Key length
        sizeof(int) +   // Value length
        sizeof(uint);   // CRC32
}
