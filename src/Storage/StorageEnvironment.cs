using JotDB.Storage.Journal;

namespace JotDB.Storage;

public sealed class StorageEnvironment
{
    private ulong _transactionSequence;

    public Transaction CreateTransaction(ReadOnlyMemory<byte> data)
    {
        var transaction = new Transaction(15_000, WriteAheadLog)
        {
            Type = TransactionType.Insert,
            Data = data,
            Number = Interlocked.Increment(ref _transactionSequence)
        };

        return transaction;
    }
    
    public WriteAheadLog WriteAheadLog { get; } = new();

    public void FlushToDisk()
    {
        WriteAheadLog.FlushToDisk();
    }
}