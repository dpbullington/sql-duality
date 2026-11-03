namespace SqlDuality.Core.Transactions;

/// <summary>
/// Thrown when a transaction's commit is rejected because another transaction modified one of its
/// keys after this transaction captured its read snapshot (first-committer-wins snapshot isolation).
/// </summary>
public sealed class WriteConflictException : Exception
{
    /// <summary>The key whose concurrent modification caused the conflict, if known.</summary>
    public byte[]? ConflictingKey { get; }

    public WriteConflictException(string message, byte[]? conflictingKey = null)
        : base(message)
    {
        ConflictingKey = conflictingKey;
    }
}
