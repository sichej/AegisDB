
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
}
