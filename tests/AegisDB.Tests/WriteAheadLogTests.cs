using System.Text;
using AegisDB.Core.Persistence;

namespace AegisDB.Tests;

public class WriteAheadLogTests
{
    [Fact]
    public async Task AppendAsync_ShouldPersistMultipleRecords()
    {
        string filePath = Path.Combine(
            Path.GetTempPath(),
            $"{Guid.NewGuid()}.wal"
        );

        try
        {
            var wal = new WriteAheadLog(filePath);

            var firstRecord = new WalRecord(
                WalOperation.Put,
                "user:1",
                Encoding.UTF8.GetBytes("Alice")
            );

            var secondRecord = new WalRecord(
                WalOperation.Put,
                "user:2",
                Encoding.UTF8.GetBytes("Bob")
            );

            await wal.AppendAsync(firstRecord);
            await wal.AppendAsync(secondRecord);

            byte[] fileContent = await File.ReadAllBytesAsync(filePath);

            // The WAL must contain both records in append order.
            byte[] expected = [
                .. WalSerializer.Serialize(firstRecord),
                .. WalSerializer.Serialize(secondRecord)
            ];

            Assert.Equal(expected, fileContent);
        }
        finally
        {
            // Remove the temporary file even if an assertion fails.
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
    }

    [Fact]
    public async Task ReadAllAsync_ShouldRestoreRecordsInOrder()
    {
        string filePath = Path.Combine(
            Path.GetTempPath(),
            $"{Guid.NewGuid()}.wal"
        );

        try
        {
            var wal = new WriteAheadLog(filePath);

            var first = new WalRecord(
                WalOperation.Put,
                "user:1",
                Encoding.UTF8.GetBytes("Alice")
            );

            var second = new WalRecord(
                WalOperation.Delete,
                "user:1",
                null
            );

            await wal.AppendAsync(first);
            await wal.AppendAsync(second);

            var records = await wal.ReadAllAsync();

            Assert.Equal(2, records.Count);

            Assert.Equal(WalOperation.Put, records[0].Operation);
            Assert.Equal("user:1", records[0].Key);
            Assert.Equal(first.Value, records[0].Value);

            Assert.Equal(WalOperation.Delete, records[1].Operation);
            Assert.Equal("user:1", records[1].Key);
            Assert.Null(records[1].Value);
        }
        finally
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
    }
}