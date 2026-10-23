using System.Text;
using System.IO.Hashing;

namespace AegisDB.Core.Persistence;

/// <summary>
/// Serializes WAL records into a binary representation.
/// The resulting bytes can be appended to the Write-Ahead Log.
/// </summary>
public static class WalSerializer
{
    public static byte[] Serialize(WalRecord record)
    {
        byte[] keyBytes = Encoding.UTF8.GetBytes(record.Key);
        byte[] valueBytes = record.Value ?? [];

        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);


        long valueLength = record.Value?.LongLength ?? 0;

        long payloadLength =
            WalLimits.RecordOverheadBytes +
            keyBytes.LongLength +
            valueLength;

        if (payloadLength > WalLimits.MaxRecordPayloadBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(record),
                $"WAL record payload exceeds the maximum of " +
                $"{WalLimits.MaxRecordPayloadBytes} bytes.");
        }


        // Reserve space for the record length.
        writer.Write(0);

        writer.Write((byte)record.Operation);
        writer.Write(keyBytes.Length);
        writer.Write(valueBytes.Length);
        writer.Write(keyBytes);
        writer.Write(valueBytes);

        writer.Flush();

        byte[] data = stream.ToArray();

        // Calculate the checksum over the record payload,
        // excluding the length prefix and the checksum itself.
        // Generate 4-byte CRC32 checksum to ensure data integrity.
        uint checksum = Crc32.HashToUInt32(data.AsSpan(sizeof(int)));

        writer.Write(checksum);

        int recordLength = checked((int)stream.Length - sizeof(int));

        // Update the length prefix after completing the record.
        stream.Position = 0;
        writer.Write(recordLength);

        writer.Flush();

        return stream.ToArray();
    }

    public static WalRecord Deserialize(byte[] data)
    {
        ArgumentNullException.ThrowIfNull(data);

        const int minRecordSize = 17;

        if (data.Length < minRecordSize)
            throw new InvalidDataException("WAL record is too short.");

        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);

        int recordLength = reader.ReadInt32();

        if (recordLength != data.Length - sizeof(int))
            throw new InvalidDataException("Invalid WAL record length.");

        byte operation = reader.ReadByte();
        int keyLength = reader.ReadInt32();
        int valueLength = reader.ReadInt32();

        // Validate lengths before allocating or reading payload data.
        if (keyLength < 0 || valueLength < 0 ||
            (long)keyLength + valueLength != data.Length - minRecordSize)
        {
            throw new InvalidDataException("Invalid WAL payload lengths.");
        }

        if (!Enum.IsDefined(typeof(WalOperation), (WalOperation)operation))
            throw new InvalidDataException("Unknown WAL operation.");

        if ((WalOperation)operation == WalOperation.Delete && valueLength != 0)
            throw new InvalidDataException("DELETE cannot contain a value.");

        byte[] keyBytes = reader.ReadBytes(keyLength);
        byte[] valueBytes = reader.ReadBytes(valueLength);

        uint storedChecksum = reader.ReadUInt32();

        // Exclude the length prefix and stored checksum from the calculation.
        uint actualChecksum = Crc32.HashToUInt32(
            data.AsSpan(sizeof(int), recordLength - sizeof(uint)));

        if (storedChecksum != actualChecksum)
            throw new InvalidDataException("WAL checksum mismatch.");

        return new WalRecord(
            (WalOperation)operation,
            Encoding.UTF8.GetString(keyBytes),
            (WalOperation)operation == WalOperation.Delete ? null : valueBytes
        );
    }
}