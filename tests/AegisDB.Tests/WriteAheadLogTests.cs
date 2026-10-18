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

    [Fact]
    public async Task ReadForRecoveryAsync_IncompleteTail_ShouldPreserveValidRecords()
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
                WalOperation.Put,
                "user:2",
                Encoding.UTF8.GetBytes("Bob")
            );

            await wal.AppendAsync(first);

            long lastValidOffset = new FileInfo(filePath).Length;

            // Simulate a crash that interrupts the second record.
            byte[] serialized = WalSerializer.Serialize(second);
            byte[] incomplete = serialized[..^5];

            await using (var stream = new FileStream(
                filePath,
                FileMode.Append,
                FileAccess.Write,
                FileShare.None))
            {
                await stream.WriteAsync(incomplete);
            }

            var result = await wal.ReadForRecoveryAsync();

            Assert.Single(result.Records);

            Assert.Equal("user:1", result.Records[0].Key);
            Assert.Equal(first.Value, result.Records[0].Value);

            Assert.True(result.HasIncompleteTail);
            Assert.Equal(lastValidOffset, result.LastValidOffset);

            // Reading must not modify the original WAL.
            Assert.Equal(
                lastValidOffset + incomplete.Length,
                new FileInfo(filePath).Length
            );
        }
        finally
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
    }

    [Fact]
    public async Task RepairAsync_ShouldRemoveIncompleteTail()
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
                WalOperation.Put,
                "user:2",
                Encoding.UTF8.GetBytes("Bob")
            );

            await wal.AppendAsync(first);

            // Save the offset of the last complete record.
            long validOffset = new FileInfo(filePath).Length;

            // Simulate an interrupted write.
            byte[] incomplete = WalSerializer.Serialize(second)[..^5];

            await using (var stream = new FileStream(
                filePath,
                FileMode.Append,
                FileAccess.Write,
                FileShare.None))
            {
                await stream.WriteAsync(incomplete);
            }

            var result = await wal.ReadForRecoveryAsync();

            Assert.True(result.HasIncompleteTail);
            Assert.Equal(validOffset, result.LastValidOffset);

            await wal.RepairAsync(result.LastValidOffset);

            // The file should now contain only the valid record.
            Assert.Equal(validOffset, new FileInfo(filePath).Length);

            var restored = await wal.ReadForRecoveryAsync();

            Assert.False(restored.HasIncompleteTail);
            Assert.Single(restored.Records);
            Assert.Equal("user:1", restored.Records[0].Key);

            // Verify that new records can be appended after repair.
            await wal.AppendAsync(second);

            var finalRecords = await wal.ReadAllAsync();

            Assert.Equal(2, finalRecords.Count);
            Assert.Equal("user:2", finalRecords[1].Key);
        }
        finally
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
    }
}