using JotDB.Storage.Journal;

namespace JotDB.Storage;

public sealed class StorageEnvironment
{
    private ulong _transactionSequence;

    public WriteAheadLog WriteAheadLog { get; } = new();
    
    public Transaction CreateTransaction(
        ReadOnlyMemory<byte> data,
        TransactionType transactionType)
    {
        var transaction = new Transaction(15_000, WriteAheadLog)
        {
            Type = transactionType,
            Data = data,
            TransactionSequenceNumber = Interlocked.Increment(ref _transactionSequence)
        };

        return transaction;
    }
    
    public void FlushToDisk()
    {
        WriteAheadLog.FlushToDisk();
    }
}