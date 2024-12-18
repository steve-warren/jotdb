using System.Collections.Concurrent;
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
    private readonly ConcurrentQueue<WriteAheadLogTransaction> _queue = new();
    private readonly ManualResetEventSlim _transactionsAvailable = new(false);

    ~WriteAheadLogTransactionBuffer()
    {
        Dispose();
    }

    public void WaitForTransactions(CancellationToken cancellationToken)
    {
        _transactionsAvailable.Wait(cancellationToken);
    }

    public IEnumerable<WriteAheadLogTransaction> ReadTransactions(
            int bytes,
            CancellationToken cancellationToken)
    {
        var totalBytes = 0U;

        cancellationToken.ThrowIfCancellationRequested();

        while (_queue.TryPeek(out var transaction))
        {
            Debug.Assert(transaction.Size <= bytes,
                "Transaction size exceeds buffer size.");

            if (totalBytes + transaction.Size > bytes)
                yield break;

            _queue.TryDequeue(out _);
            totalBytes += transaction.Size;

            yield return transaction;
        }

        _transactionsAvailable.Reset();
    }

    public Task WriteTransactionAsync(WriteAheadLogTransaction transaction)
    {
        _queue.Enqueue(transaction);
        _transactionsAvailable.Set();
        return transaction.WaitForCommitAsync();
    }

    public void Dispose()
    {
        Debug.Assert(_queue.IsEmpty, "WAL transactions are still pending in the buffer when calling Dispose().");
        _transactionsAvailable.Dispose();

        GC.SuppressFinalize(this);
    }
}