using System.Runtime.CompilerServices;
using JotDB.Storage.Journal;

namespace JotDB.Storage;

/// <summary>
/// Represents a transactional operation within the system.
/// </summary>
/// <remarks>
/// A transaction encapsulates a series of operations that can be committed or rolled back as a single unit.
/// Transactions are uniquely identified by their sequence numbers and can store associated data.
/// </remarks>
public sealed class Transaction : IDisposable
{
    private readonly WriteAheadLog _wal;
    private readonly CancellationTokenSource _cts;

    public Transaction(
        int timeout,
        WriteAheadLog wal)
    {
        _wal = wal;
        _cts = new CancellationTokenSource(timeout);
    }

    public ulong Number { get; init; }
    public ulong CommitSequenceNumber { get; private set; }
    public ReadOnlyMemory<byte> Data { get; init; }
    public TransactionHeader? Header { get; private set; }
    public TransactionType Type { get; init; }

    public Task CommitAsync()
    {
        return _wal.AppendAsync(this);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void CopyTo(Span<byte> destination, int offset)
    {
        Data.Span.CopyTo(destination.Slice(offset, Data.Length));
    }

    public void Dispose()
    {
        _cts.Cancel();
        _cts.Dispose();
    }
}