using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using JotDB.Threading;

namespace JotDB.Storage;

public sealed class Transaction : IDisposable
{
    private readonly TransactionBuffer _transactionBuffer;
    private readonly AsyncAwaiter _commitAwaiter;
    private readonly CancellationTokenSource _cts;

    public Transaction(
        int timeout,
        TransactionBuffer transactionBuffer)
    {
        _transactionBuffer = transactionBuffer;
        _cts = new CancellationTokenSource(timeout);
        _commitAwaiter = new AsyncAwaiter(_cts.Token);
    }

    public ulong Number { get; init; }
    public ulong CommitSequenceNumber { get; private set; }
    public ReadOnlyMemory<byte> Data { get; init; }
    public TransactionHeader? Header { get; private set; }
    public TransactionType Type { get; init; }
    public bool IsCommitted => _commitAwaiter.IsSet;

    public async Task CommitAsync()
    {
        Header = new TransactionHeader
        {
            TransactionSequenceNumber = Number,
            DataLength = Data.Length
        };

        // place the transaction in the stream and wait for the commit to complete.
        await _transactionBuffer.WriteTransactionAsync(this)
            .ConfigureAwait(false);
        await _commitAwaiter.WaitForSignalAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Completes the current transaction's commit process after the specified task is completed.
    /// </summary>
    /// <param name="waiter">The task to wait for before signaling the commit completion.</param>
    internal void FinalizeCommit(Task waiter)
    {
        _ = waiter.ContinueWith((_, o) =>
        {
            var mre = (AsyncAwaiter)o!;

            mre.SignalCompletion();
        }, _commitAwaiter, TaskContinuationOptions.ExecuteSynchronously);
    }

    public void Abort(Exception exception)
    {
        _commitAwaiter.SignalFault(exception);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void CopyTo(Span<byte> destination, int offset)
    {
        Data.Span.CopyTo(destination.Slice(offset, Data.Length));
    }

    public void Dispose()
    {
        _cts.Cancel();
        _commitAwaiter.Dispose();
        _cts.Dispose();
    }
}