using System.Text;
using AegisDB.Core.Persistence;

namespace AegisDB.Tests;

public class WalSerializerTests
{
    [Fact]
    public void SerializeDeserialize_ShouldPreserveRecord()
    {
        var original = new WalRecord(
            WalOperation.Put,
            "user:1",
            Encoding.UTF8.GetBytes("Alice")
        );

        byte[] serialized = WalSerializer.Serialize(original);
        WalRecord restored = WalSerializer.Deserialize(serialized);

        Assert.Equal(original.Operation, restored.Operation);
        Assert.Equal(original.Key, restored.Key);
        Assert.Equal(original.Value, restored.Value);
    }

    [Fact]
    public void Deserialize_CorruptedData_ShouldThrowException()
    {
        var record = new WalRecord(
            WalOperation.Put,
            "user:1",
            Encoding.UTF8.GetBytes("Alice")
        );

        byte[] serialized = WalSerializer.Serialize(record);

        // Corrupt one byte in the value without updating the checksum.
        serialized[20] ^= 0xFF;

        Assert.Throws<InvalidDataException>(() =>
            WalSerializer.Deserialize(serialized));
    }


    [Fact]
    public void Serialize_ShouldRejectRecordExceedingReaderLimit()
    {
        const string key = "test";

        int maximumValueLength =
            WalLimits.MaxRecordPayloadBytes -
            WalLimits.RecordOverheadBytes -
            Encoding.UTF8.GetByteCount(key);

        var oversizedRecord = new WalRecord(
            WalOperation.Put,
            key,
            new byte[maximumValueLength + 1]);

        Assert.Throws<ArgumentOutOfRangeException>(
            () => WalSerializer.Serialize(oversizedRecord));
    }


    [Fact]
    public void Serialize_ShouldAcceptRecordAtExactReaderLimit()
    {
        const string key = "test";

        int maximumValueLength =
            WalLimits.MaxRecordPayloadBytes -
            WalLimits.RecordOverheadBytes -
            Encoding.UTF8.GetByteCount(key);

        var record = new WalRecord(
            WalOperation.Put,
            key,
            new byte[maximumValueLength]);

        byte[] serialized = WalSerializer.Serialize(record);

        // The four-byte length prefix is outside the payload limit.
        Assert.Equal(
            WalLimits.MaxRecordPayloadBytes + sizeof(int),
            serialized.Length);
    }


}