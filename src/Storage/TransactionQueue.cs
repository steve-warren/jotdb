using System.Threading.Channels;

namespace JotDB.Storage;

public sealed class TransactionQueue : IDisposable
{
    private const int JOURNAL_MEMORY_BUFFER_SIZE = 128;
    private readonly Channel<Transaction> _pendingTransactions;

    public TransactionQueue()
    {
        _pendingTransactions = Channel.CreateBounded<Transaction>(
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

    public bool TryPeek(out Transaction? transaction) =>
        _pendingTransactions.Reader.TryPeek(out transaction);

    public bool TryDequeue(out Transaction? transaction) =>
        _pendingTransactions.Reader.TryRead(out transaction);

    public ValueTask EnqueueAsync(Transaction transaction) =>
        _pendingTransactions.Writer.WriteAsync(transaction);

    public void Dispose() =>
        _pendingTransactions.Writer.TryComplete();
}