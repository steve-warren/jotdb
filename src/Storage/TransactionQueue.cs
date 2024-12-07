using System.Threading.Channels;

namespace JotDB.Storage;

public sealed class TransactionQueue : IDisposable
{
    private const int JOURNAL_MEMORY_BUFFER_SIZE = 128;
    private readonly Channel<StorageTransaction> _pendingTransactions;

    public TransactionQueue()
    {
        _pendingTransactions = Channel.CreateBounded<StorageTransaction>(
            new BoundedChannelOptions(JOURNAL_MEMORY_BUFFER_SIZE)
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = true,
                FullMode = BoundedChannelFullMode.Wait
            });
    }

    public ValueTask<bool> WaitAsync(CancellationToken cancellationToken = default) =>
        _pendingTransactions.Reader.WaitToReadAsync(cancellationToken);

    public IEnumerable<StorageTransaction> GetConsumingEnumerable()
    {
        while (_pendingTransactions.Reader.TryRead(out var transaction))
            yield return transaction;
    }

    public ValueTask EnqueueAsync(StorageTransaction storageTransaction) =>
        _pendingTransactions.Writer.WriteAsync(storageTransaction);

    public void Dispose() =>
        _pendingTransactions.Writer.TryComplete();
}