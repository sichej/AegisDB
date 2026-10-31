
using AegisDB.Core.Persistence;

namespace AegisDB.Core.Consensus;

/// <summary>
/// Validates the semantic correctness of commands stored
/// in the Raft replicated log.
/// </summary>
public static class RaftCommandValidator
{
    /// <summary>
    /// Returns whether a command has valid operation,
    /// key, and value semantics.
    /// </summary>
    public static bool IsValid(WalRecord? command)
    {
        if (command is null ||
            string.IsNullOrEmpty(command.Key))
        {
            return false;
        }

        return command.Operation switch
        {
            WalOperation.Put => command.Value is not null,
            WalOperation.Delete => command.Value is null,
            _ => false
        };
    }

    /// <summary>
    /// Rejects malformed commands before they can be persisted
    /// or published into the Raft log.
    /// </summary>
    public static void Validate(WalRecord? command)
    {
        if (!IsValid(command))
        {
            throw new ArgumentException(
                "Invalid Raft command: operation, key, or value.",
                nameof(command));
        }
    }
}
