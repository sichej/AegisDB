
using AegisDB.Core.Consensus;
using AegisDB.Core.Persistence;

namespace AegisDB.Tests;

public sealed class RaftLogTests
{
    private static RaftLogEntry CreateEntry(long index, long term)
    {
        return new RaftLogEntry(
            index,
            term,
            new WalRecord(
                WalOperation.Put,
                $"key-{index}",
                [1, 2, 3]));
    }

    [Fact]
    public void NewLog_ShouldBeEmpty()
    {
        var log = new RaftLog();

        Assert.Equal(0, log.LastIndex);
        Assert.Equal(0, log.LastTerm);
        Assert.True(log.Matches(0, 0));
    }

    [Fact]
    public void Append_ShouldMaintainSequentialIndices()
    {
        var log = new RaftLog();

        log.Append(CreateEntry(1, 1));
        log.Append(CreateEntry(2, 1));
        log.Append(CreateEntry(3, 2));

        Assert.Equal(3, log.LastIndex);
        Assert.Equal(2, log.LastTerm);
        Assert.Equal(2, log.GetEntry(3)?.Term);
    }

    [Fact]
    public void Append_WithInvalidIndex_ShouldThrow()
    {
        var log = new RaftLog();

        Assert.Throws<InvalidOperationException>(
            () => log.Append(CreateEntry(2, 1)));
    }

    [Fact]
    public void Matches_ShouldCheckIndexAndTerm()
    {
        var log = new RaftLog();

        log.Append(CreateEntry(1, 1));
        log.Append(CreateEntry(2, 2));

        Assert.True(log.Matches(1, 1));
        Assert.True(log.Matches(2, 2));
        Assert.False(log.Matches(2, 1));
        Assert.False(log.Matches(3, 2));
    }


    [Fact]
    public void TruncateFrom_ShouldRemoveConflictingSuffix()
    {
        var log = new RaftLog();

        for (long index = 1; index <= 5; index++)
        {
            log.Append(CreateEntry(index, index <= 3 ? 1 : 2));
        }

        log.TruncateFrom(4);

        Assert.Equal(3, log.LastIndex);
        Assert.Equal(1, log.LastTerm);

        Assert.NotNull(log.GetEntry(3));
        Assert.Null(log.GetEntry(4));
        Assert.Null(log.GetEntry(5));
    }

    [Fact]
    public void TruncateFrom_FirstIndex_ShouldClearLog()
    {
        var log = new RaftLog();

        log.Append(CreateEntry(1, 1));
        log.Append(CreateEntry(2, 1));

        log.TruncateFrom(1);

        Assert.Equal(0, log.LastIndex);
        Assert.Equal(0, log.LastTerm);
    }

    [Fact]
    public void TruncateFrom_ShouldAllowAppendingReplacementEntries()
    {
        var log = new RaftLog();

        log.Append(CreateEntry(1, 1));
        log.Append(CreateEntry(2, 1));
        log.Append(CreateEntry(3, 2));

        log.TruncateFrom(3);

        // The new entry replaces the conflicting index 3.
        log.Append(CreateEntry(3, 3));

        Assert.Equal(3, log.LastIndex);
        Assert.Equal(3, log.LastTerm);
        Assert.Equal(3, log.GetEntry(3)?.Term);
    }

    [Fact]
    public void TruncateFrom_BeyondLastIndex_ShouldThrow()
    {
        var log = new RaftLog();

        log.Append(CreateEntry(1, 1));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => log.TruncateFrom(3));
    }


    [Fact]
    public void AppendEntries_ShouldReplaceConflictingSuffix()
    {
        var log = new RaftLog();

        log.Append(CreateEntry(1, 1));
        log.Append(CreateEntry(2, 1));
        log.Append(CreateEntry(3, 2));
        log.Append(CreateEntry(4, 2));
        log.Append(CreateEntry(5, 2));

        log.AppendEntries(
        [
            CreateEntry(4, 3),
        CreateEntry(5, 3)
        ]);

        Assert.Equal(5, log.LastIndex);
        Assert.Equal(3, log.LastTerm);

        Assert.Equal(2, log.GetEntry(3)?.Term);
        Assert.Equal(3, log.GetEntry(4)?.Term);
        Assert.Equal(3, log.GetEntry(5)?.Term);
    }

    [Fact]
    public void AppendEntries_WithMatchingEntries_ShouldNotDuplicate()
    {
        var log = new RaftLog();

        log.Append(CreateEntry(1, 1));
        log.Append(CreateEntry(2, 2));

        log.AppendEntries(
        [
            CreateEntry(1, 1),
        CreateEntry(2, 2)
        ]);

        Assert.Equal(2, log.LastIndex);
        Assert.Equal(2, log.LastTerm);
    }

    [Fact]
    public void AppendEntries_ShouldAppendMissingEntries()
    {
        var log = new RaftLog();

        log.Append(CreateEntry(1, 1));
        log.Append(CreateEntry(2, 1));

        log.AppendEntries(
        [
            CreateEntry(3, 2),
        CreateEntry(4, 2)
        ]);

        Assert.Equal(4, log.LastIndex);
        Assert.Equal(2, log.LastTerm);
    }

    [Fact]
    public void AppendEntries_WithNonConsecutiveIndices_ShouldThrow()
    {
        var log = new RaftLog();

        Assert.Throws<ArgumentException>(() =>
            log.AppendEntries(
            [
                CreateEntry(1, 1),
            CreateEntry(3, 2)
            ]));
    }


    [Fact]
    public void AppendEntries_WithInvalidSequence_ShouldPreserveOriginalLog()
    {
        var log = new RaftLog();

        log.Append(CreateEntry(1, 1));
        log.Append(CreateEntry(2, 1));
        log.Append(CreateEntry(3, 2));
        log.Append(CreateEntry(4, 2));
        log.Append(CreateEntry(5, 2));

        Assert.Throws<ArgumentException>(() =>
            log.AppendEntries(
            [
                CreateEntry(4, 3),
            CreateEntry(5, 3),
            CreateEntry(7, 3)
            ]));

        // No existing entries should have been modified.
        Assert.Equal(5, log.LastIndex);
        Assert.Equal(2, log.LastTerm);
        Assert.Equal(2, log.GetEntry(4)?.Term);
        Assert.Equal(2, log.GetEntry(5)?.Term);
    }

    [Fact]
    public void AppendEntries_WithMissingPredecessor_ShouldPreserveLog()
    {
        var log = new RaftLog();

        log.Append(CreateEntry(1, 1));
        log.Append(CreateEntry(2, 1));

        Assert.Throws<InvalidOperationException>(() =>
            log.AppendEntries(
            [
                CreateEntry(5, 3),
            CreateEntry(6, 3)
            ]));

        Assert.Equal(2, log.LastIndex);
        Assert.Equal(1, log.LastTerm);
    }

    [Fact]
    public void Append_ShouldDefensivelyCopyCommandValue()
    {
        var log = new RaftLog();

        byte[] original = "Alice"u8.ToArray();

        log.Append(new RaftLogEntry(
            1,
            1,
            new WalRecord(WalOperation.Put, "user:1", original)));

        original[0] = (byte)'M';

        var stored = log.GetEntry(1);

        Assert.Equal(
            "Alice",
            System.Text.Encoding.UTF8.GetString(
                stored!.Command.Value!));
    }

}
