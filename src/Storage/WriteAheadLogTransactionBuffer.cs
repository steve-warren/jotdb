using System.Threading.Channels;
using JotDB.Storage.Journal;

namespace JotDB.Storage;

/// <summary>
/// Provides a buffering mechanism for handling write-ahead log transactions in a bounded, thread-safe manner.
/// </summary>
/// <remarks>
/// This class acts as a mediator for transactions awaiting to be processed and written to a write-ahead log.
/// It utilizes a bounded channel to support efficient producer-consumer workflows, ensuring safe concurrency
/// between writers (producers) and the single reader (consumer). The channel is configured to block producers
/// when the buffer is full to prevent unbounded memory usage.
/// </remarks>
/// <threadsafety>
/// This class is thread-safe for multiple writers and a single reader.
/// </threadsafety>
/// <example>
/// The transaction buffer maintains order and ensures proper writing and committing mechanisms
/// for transactions logged in the Write Ahead Log (WAL). Consumers can enumerate transactions
/// as they become available, while producers asynchronously enqueue new transactions.
/// </example>
/// <seealso cref="WriteAheadLog" />
public sealed class WriteAheadLogTransactionBuffer : IDisposable
{
    private readonly Channel<WriteAheadLogTransaction> _channel = Channel.CreateBounded<WriteAheadLogTransaction>(
        new BoundedChannelOptions(Environment.ProcessorCount * 2)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });

    public ValueTask<bool> WaitForTransactionsAsync(CancellationToken cancellationToken = default) =>
        _channel.Reader.WaitToReadAsync(cancellationToken);

    public IEnumerable<WriteAheadLogTransaction> ReadTransactions()
    {
        while (_channel.Reader.TryRead(out var transaction))
            yield return transaction;
    }

    public Task WriteTransactionAsync(WriteAheadLogTransaction transaction)
    {
        return _channel.Writer.WriteAsync(transaction).AsTask().ContinueWith((_, trx) =>
        {
            var pendingTransaction = (WriteAheadLogTransaction)trx;

            return pendingTransaction.WaitForCommitAsync();
        }, transaction, TaskContinuationOptions.ExecuteSynchronously).Unwrap();
    }

    public void Dispose() =>
        _channel.Writer.TryComplete();
}