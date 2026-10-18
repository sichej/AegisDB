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
}