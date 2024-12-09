using System.Diagnostics;
using System.Runtime.CompilerServices;
using JotDB.Threading;

namespace JotDB.Storage;

public sealed class StorageTransaction : IDisposable
{
    private readonly AsyncManualResetEvent _mre;
    private readonly CancellationTokenSource _cts;

    public StorageTransaction(int timeout)
    {
        _cts = new CancellationTokenSource(timeout);
        _mre = new AsyncManualResetEvent(_cts.Token);
    }

    public ReadOnlyMemory<byte> Data { get; init; }

    public TransactionType Type { get; init; }
    public bool IsCommitted => _mre.IsSet;

    /// <summary>
    /// Marks the journal entry as written to disk and sets the task result.
    /// </summary>
    public void Commit()
    {
        _mre.Set();
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

    public Task WaitAsync()
    {
        return _mre.WaitAsync();
    }

    public void Dispose()
    {
        _cts.Cancel();
        _mre.Dispose();
        _cts.Dispose();
    }
}