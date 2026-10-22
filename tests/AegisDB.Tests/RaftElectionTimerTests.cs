
using AegisDB.Core.Consensus;

namespace AegisDB.Tests;

public sealed class RaftElectionTimerTests
{
    [Fact]
    public async Task Timer_ShouldExpireWithoutReset()
    {
        var timer = new RaftElectionTimer(
            TimeSpan.FromMilliseconds(20),
            TimeSpan.FromMilliseconds(40));

        bool timedOut = await timer.WaitForTimeoutAsync();

        Assert.True(timedOut);
    }

    [Fact]
    public async Task Timer_Reset_ShouldCancelPendingWait()
    {
        var timer = new RaftElectionTimer(
            TimeSpan.FromSeconds(5),
            TimeSpan.FromSeconds(6));

        Task<bool> wait = timer.WaitForTimeoutAsync();

        timer.Reset();

        bool timedOut = await wait;

        Assert.False(timedOut);
    }
}
