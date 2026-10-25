using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Messages;
using AegisDB.Core.Consensus.Persistence;
using AegisDB.Core.Consensus.Transport;
using AegisDB.Core.Persistence;
using AegisDB.Server.Monitoring;

namespace AegisDB.Tests;

public sealed class LocalRaftTransportTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StoppingSenderOrTarget_ShouldCancelAndDrainAdmittedRpc(bool stopSender)
    {
        string directory = Path.Combine(Path.GetTempPath(), $"aegis-local-transport-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        using var machine = new BlockingStateMachine();
        using var lifetime = new NodeLifetime();
        using var sourceLifetime = new NodeLifetime();
        Task? application = null;
        try
        {
            var node = await RaftNode.CreateAsync("node-2",
                new RaftClusterConfiguration(["node-1", "node-2", "node-3"]),
                new RaftStateStore(Path.Combine(directory, "state.json")),
                new RaftLogStore(Path.Combine(directory, "log.json")), stateMachine: machine);
            var transport = new LocalRaftTransport();
            transport.Register(node, lifetime);
            await node.HandleAppendEntriesAsync(new AppendEntriesRequest(
                1, "node-1", 0, 0,
                [new RaftLogEntry(1, 1, new WalRecord(WalOperation.Put, "key", [1]))], 1));
            application = Task.Run(() => node.ApplyCommittedEntriesAsync(lifetime.Token));
            await machine.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));

            var sourceTransport = transport.ForNode(sourceLifetime);
            Task<RequestVoteResponse> rpc = sourceTransport.RequestVoteAsync(
                "node-2", new RequestVoteRequest(1, "node-3", 1, 1));
            Assert.False(rpc.IsCompleted);
            if (stopSender)
            {
                sourceLifetime.StopAccepting();
                await sourceLifetime.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => rpc);
                await sourceLifetime.Drained.WaitAsync(TimeSpan.FromSeconds(5));
                Assert.True(lifetime.IsAvailable);
                Assert.Null(sourceLifetime.TryAcquire());
            }
            else
            {
                lifetime.StopAccepting();
                transport.Unregister("node-2");
                await lifetime.CancelAsync();
                await Assert.ThrowsAsync<RaftTransportException>(() => rpc);
                await lifetime.Drained.WaitAsync(TimeSpan.FromSeconds(5));
                Assert.Null(lifetime.TryAcquire());
                await Assert.ThrowsAsync<RaftTransportException>(() => transport.RequestVoteAsync(
                    "node-2", new RequestVoteRequest(1, "node-3", 1, 1)));
            }
        }
        finally
        {
            machine.Release.Set();
            if (application is not null) await application;
            Directory.Delete(directory, recursive: true);
        }
    }

    private sealed class BlockingStateMachine : IRaftStateMachine, IDisposable
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ManualResetEventSlim Release { get; } = new();
        public void Apply(RaftLogEntry entry)
        {
            Entered.TrySetResult();
            if (!Release.Wait(TimeSpan.FromSeconds(10)))
                throw new TimeoutException("Test did not release the blocked state machine.");
        }
        public void Dispose() => Release.Dispose();
    }
}
