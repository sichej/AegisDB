
using System.Text;
using AegisDB.Core.Consensus;
using AegisDB.Core.Persistence;
using AegisDB.Core.Storage;

namespace AegisDB.Tests;

public sealed class RaftStateMachineTests
{
    [Fact]
    public void ApplyPut_ShouldStoreValue()
    {
        var store = new KeyValueStore();
        var stateMachine = new RaftStateMachine(store);

        var entry = new RaftLogEntry(
            Index: 1,
            Term: 1,
            Command: new WalRecord(
                WalOperation.Put,
                "user:1",
                Encoding.UTF8.GetBytes("Alice")));

        stateMachine.Apply(entry);

        Assert.Equal(
            "Alice",
            Encoding.UTF8.GetString(store.Get("user:1")!));
    }

    [Fact]
    public void ApplyDelete_ShouldRemoveValue()
    {
        var store = new KeyValueStore();
        var stateMachine = new RaftStateMachine(store);

        store.Put("user:1", Encoding.UTF8.GetBytes("Alice"));

        var entry = new RaftLogEntry(
            Index: 2,
            Term: 1,
            Command: new WalRecord(
                WalOperation.Delete,
                "user:1",
                null));

        stateMachine.Apply(entry);

        Assert.Null(store.Get("user:1"));
    }
}
