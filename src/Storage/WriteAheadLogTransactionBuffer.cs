using System.Diagnostics;
using System.Runtime.CompilerServices;
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
    private readonly Channel<WriteAheadLogTransaction> _channel =
        Channel.CreateBounded<WriteAheadLogTransaction>(
            new BoundedChannelOptions(Environment.ProcessorCount * 2)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait
            });

    public ValueTask<bool> WaitForTransactionsAsync(
        CancellationToken cancellationToken = default) =>
        _channel.Reader.WaitToReadAsync(cancellationToken);

    public async IAsyncEnumerable<WriteAheadLogTransaction>
        ReadTransactionsAsync(
            int bytes,
            TimeSpan timeout,
            [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        uint totalBytes = 0;
        var timeoutTask = Task.Delay(timeout, cancellationToken);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            while (_channel.Reader.TryPeek(out var transaction))
            {
                if (timeoutTask.IsCompleted)
                    yield break;

                Debug.Assert(transaction.Size <= bytes,
                    "Transaction size exceeds buffer size.");

                if (totalBytes + transaction.Size > bytes)
                    yield break;

                yield return transaction;
                _channel.Reader.TryRead(out _);
                totalBytes += transaction.Size;
            }

            var readTask = _channel.Reader.WaitToReadAsync(cancellationToken)
                .AsTask();
            var completedTask = await Task.WhenAny(timeoutTask, readTask)
                .ConfigureAwait(false);

            if (completedTask == timeoutTask)
                yield break;
        }
    }

    public Task WriteTransactionAsync(WriteAheadLogTransaction transaction)
    {
        return _channel.Writer.WriteAsync(transaction).AsTask().ContinueWith(
                (_, trx) =>
                {
                    var pendingTransaction = (WriteAheadLogTransaction)trx!;

                    return pendingTransaction.WaitForCommitAsync();
                }, transaction, TaskContinuationOptions.ExecuteSynchronously)
            .Unwrap();
    }

    public void Dispose() =>
        _channel.Writer.TryComplete();
}