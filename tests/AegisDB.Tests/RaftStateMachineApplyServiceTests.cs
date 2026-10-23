
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Messages;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Persistence;

namespace AegisDB.Tests;

public sealed class RaftStateMachineApplyServiceTests
{
    [Fact]
    public async Task Follower_ShouldAutomaticallyApplyCommittedEntries()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-apply-service",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        using var cts = new CancellationTokenSource(
            TimeSpan.FromSeconds(5));

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var logStore = new RaftLogStore(
                Path.Combine(directory, "log.json"));

            await logStore.SaveAsync(
            [
                new RaftLogEntry(
                    Index: 1,
                    Term: 1,
                    Command: new WalRecord(
                        WalOperation.Put,
                        "user:1",
                        [65, 108, 105, 99, 101])),

                new RaftLogEntry(
                    Index: 2,
                    Term: 1,
                    Command: new WalRecord(
                        WalOperation.Put,
                        "user:2",
                        [66, 111, 98]))
            ]);

            var follower = await RaftNode.CreateAsync(
                "node-2",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "state.json")),
                logStore);

            var service = new RaftStateMachineApplyService(
                follower,
                TimeSpan.FromMilliseconds(10));

            Task serviceTask = service.RunAsync(cts.Token);

            try
            {
                // The follower knows the entries, but they are
                // not committed or applied yet.
                var initial = await follower.GetStateSnapshotAsync();

                Assert.Equal(2, initial.LastLogIndex);
                Assert.Equal(0, initial.CommitIndex);
                Assert.Equal(0, initial.LastApplied);

                // The leader announces that both entries are committed.
                var response = await follower.HandleAppendEntriesAsync(
                    new AppendEntriesRequest(
                        Term: 1,
                        LeaderId: "node-1",
                        PrevLogIndex: 2,
                        PrevLogTerm: 1,
                        Entries: [],
                        LeaderCommit: 2));

                Assert.True(response.Success);

                // Observe the service until it applies both entries.
                using var waitCts =
                    CancellationTokenSource.CreateLinkedTokenSource(
                        cts.Token);

                waitCts.CancelAfter(TimeSpan.FromSeconds(2));

                RaftNodeStateSnapshot snapshot;

                do
                {
                    snapshot = await follower.GetStateSnapshotAsync(
                        waitCts.Token);

                    if (snapshot.LastApplied == 2)
                        break;

                    await Task.Delay(10, waitCts.Token);
                }
                while (true);

                Assert.Equal(2, snapshot.CommitIndex);
                Assert.Equal(2, snapshot.LastApplied);
                Assert.Equal(2, snapshot.LastLogIndex);

                // A later observation must not move LastApplied back.
                var repeated = await follower.GetStateSnapshotAsync();

                Assert.Equal(2, repeated.LastApplied);
            }
            finally
            {
                cts.Cancel();

                try
                {
                    await serviceTask;
                }
                catch (OperationCanceledException)
                    when (cts.IsCancellationRequested)
                {
                    // Expected when stopping the apply service.
                }
            }
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
