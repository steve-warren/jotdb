using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace JotDB.Storage.Journal;

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
    private readonly SemaphoreSlim _transactionsAvailable = new(0);

    ~WriteAheadLogTransactionBuffer()
    {
        Dispose();
    }

    public int Count => _queue.Count;

    /// <summary>
    /// Waits until transactions are available in the buffer or the operation is canceled.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting that allows the operation to be canceled.</param>
    public void Wait(CancellationToken cancellationToken)
    {
        _transactionsAvailable.Wait(cancellationToken);
    }

    /// <summary>
    /// Appends a WriteAheadLogTransaction to the transaction buffer.
    /// </summary>
    /// <param name="transaction">The transaction to be appended to the buffer.</param>
    public void Append(WriteAheadLogTransaction transaction)
    {
        try
        {
            _queue.Enqueue(transaction);
        }

        finally
        {
            // wake the writer thread to process the transaction we just placed in the queue
            _transactionsAvailable.Release();
        }
    }

    public void Dispose()
    {
        Debug.Assert(_queue.IsEmpty,
            "WAL transactions are still pending in the buffer when calling Dispose().");
        _transactionsAvailable.Dispose();

        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Represents an enumerator for iterating over transactions in the WriteAheadLogTransactionBuffer
    /// while respecting a specified byte limit and handling cancellation tokens.
    /// </summary>
    /// <remarks>
    /// This struct provides a mechanism to sequentially process transactions stored in the WriteAheadLogTransactionBuffer
    /// while enforcing a maximum byte limit for the transactions to be retrieved. The enumerator also supports canceling
    /// the iteration process through a CancellationToken to ensure responsiveness to cancellation requests.
    /// </remarks>
    /// <threadsafety>
    /// This struct is not thread-safe and should not be accessed concurrently from multiple threads.
    /// </threadsafety>
    /// <seealso cref="WriteAheadLogTransactionBuffer" />
    /// <seealso cref="JotDB.Storage.Journal.WriteAheadLogTransaction" />
    public ref struct ConsumableEnumerator
    {
        const int MAX_SPIN_COUNT = 10;

        private readonly WriteAheadLogTransactionBuffer _buffer;
        private readonly int _bytes;
        private uint _totalBytes = 0U;
        private bool _disposed;

        public ConsumableEnumerator(
            WriteAheadLogTransactionBuffer buffer,
            int bytes)
        {
            _buffer = buffer;
            _bytes = bytes;
        }

        public void Dispose()
        {
            if (!_disposed)
                return;

            _disposed = true;
        }

        public bool MoveNext(
            [MaybeNullWhen(false)] out WriteAheadLogTransaction transaction)
        {
            var spinCount = 0;

            tryPeek:
            if (_buffer._queue.TryPeek(out transaction))
            {
                if (_totalBytes + transaction.Size > _bytes)
                    return false;

                _buffer._queue.TryDequeue(out _);
                _totalBytes += transaction.Size;

                Debug.Assert(_totalBytes <= _bytes,
                    "WAL transaction size exceeds the specified limit.");

                return true;
            }

            if (spinCount >= MAX_SPIN_COUNT) return false;
            Thread.SpinWait(1);
            spinCount++;
            goto tryPeek;
        }
    }
}