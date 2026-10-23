
using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Consensus.Transport;

namespace AegisDB.Tests;

public sealed class RaftElectionServiceTests
{
    [Fact]
    public async Task FailedElection_ShouldWaitForAnotherTimeout()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            "aegisdb-election-service",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        using var cts = new CancellationTokenSource();

        try
        {
            var cluster = new RaftClusterConfiguration(
                ["node-1", "node-2", "node-3"]);

            var timer = new RaftElectionTimer(
                TimeSpan.FromMilliseconds(500),
                TimeSpan.FromMilliseconds(600));

            var node = await RaftNode.CreateAsync(
                "node-1",
                cluster,
                new RaftStateStore(
                    Path.Combine(directory, "state.json")),
                electionTimer: timer);

            // No other nodes are registered, so quorum is impossible.
            var transport = new InMemoryRaftTransport();

            var coordinator = new RaftElectionCoordinator(
                node,
                transport,
                ["node-2", "node-3"]);

            var service = new RaftElectionService(
                node,
                timer,
                coordinator);

            Task serviceTask = service.RunAsync(cts.Token);

            try
            {
                // Wait for the first election to begin.
                using var timeout = new CancellationTokenSource(
                    TimeSpan.FromSeconds(3));

                while ((await node.GetStateSnapshotAsync(
                    timeout.Token)).CurrentTerm == 0)
                {
                    await Task.Delay(10, timeout.Token);
                }

                long firstTerm =
                    (await node.GetStateSnapshotAsync()).CurrentTerm;

                Assert.Equal(1, firstTerm);

                // Less than the minimum election timeout:
                // no second election should have started.
                await Task.Delay(100);

                var snapshot = await node.GetStateSnapshotAsync();

                Assert.Equal(1, snapshot.CurrentTerm);
                Assert.Equal(RaftNodeState.Candidate, snapshot.State);
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
                    // Expected during shutdown.
                }
            }
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
