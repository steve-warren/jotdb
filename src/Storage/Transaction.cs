using System.Runtime.CompilerServices;
using JotDB.Threading;

namespace JotDB.Storage;

public sealed class Transaction : IDisposable
{
    private readonly TransactionStream _transactionStream;
    private readonly AsyncManualResetEvent _mre;
    private readonly CancellationTokenSource _cts;

    public Transaction(int timeout, TransactionStream transactionStream)
    {
        _transactionStream = transactionStream;
        _cts = new CancellationTokenSource(timeout);
        _mre = new AsyncManualResetEvent(_cts.Token);
    }

    public ulong Number { get; init; }
    public ulong CommitSequenceNumber { get; private set; }
    public ReadOnlyMemory<byte> Data { get; init; }

    public TransactionType Type { get; init; }
    public bool IsCommitted => _mre.IsSet;

    public async Task CommitAsync()
    {
        await _transactionStream.WriteTransactionAsync(this).ConfigureAwait(false);
        await _mre.WaitAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Signals the completion of the current transaction's commit process after the specified task is completed.
    /// </summary>
    /// <param name="waiter">The task to wait for before signaling the commit completion.</param>
    /// <returns>A task that represents the asynchronous operation, which completes once the signal is set.</returns>
    internal Task SignalCommitCompletionAfter(Task waiter)
    {
        return waiter.ContinueWith((_, o) =>
        {
            var mre = (AsyncManualResetEvent)o!;

            mre.SetCompleted();
        }, _mre, TaskContinuationOptions.ExecuteSynchronously);
    }

    public void Rollback(Exception exception)
    {
        _mre.SetException(exception);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void CopyTo(Span<byte> destination, int offset)
    {
        Data.Span.CopyTo(destination.Slice(offset, Data.Length));
    }

    public void Dispose()
    {
        _cts.Cancel();
        _mre.Dispose();
        _cts.Dispose();
    }
}