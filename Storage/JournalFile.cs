using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using Microsoft.Win32.SafeHandles;

namespace JotDB.Storage;

public sealed class JournalFile : IDisposable
{
    private readonly SafeFileHandle _file;
    private ulong _transactionId;
    private readonly TransactionQueue _pendingTransactions;
    private long _offset;

    public static JournalFile Open(
        string path)
    {
        return new JournalFile(
            path: path,
            offset: 0);
    }

    private static SafeFileHandle OpenFileHandle(string path)
    {
        const FileOptions FILE_FLAG_NO_BUFFERING = (FileOptions)0x20000000;
        var fileOptions = FileOptions.WriteThrough;

        if (OperatingSystem.IsWindows())
            fileOptions |= FILE_FLAG_NO_BUFFERING;

        return File.OpenHandle(
            path: path,
            mode: FileMode.OpenOrCreate,
            access: FileAccess.ReadWrite,
            share: FileShare.None,
            options: fileOptions);
    }

    private JournalFile(
        string path,
        long offset)
    {
        Path = path;
        _offset = offset;
        _file = OpenFileHandle(path);
        _pendingTransactions = new TransactionQueue();
    }

    public string Path { get; }

    public void Dispose()
    {
        _pendingTransactions.Dispose();
        _file.Dispose();
    }

    /// <summary>
    /// Asynchronously writes an entry to the journal file.
    /// </summary>
    /// <param name="data">The data to be written to the journal file.</param>
    /// <param name="entryType">The type of journal entry operation (Insert, Update, Delete).</param>
    /// <returns>A task that represents the asynchronous write operation. The task result contains the unique operation ID of the written operation.</returns>
    public Task WriteAsync(
        ReadOnlyMemory<byte> data,
        TransactionType entryType)
    {
        var transaction = new Transaction
        {
            Data = data,
            Type = entryType
        };

        return Task.WhenAll(
            _pendingTransactions.EnqueueAsync(transaction).AsTask(),
            transaction.WaitAsync(CancellationToken.None)
        );
    }

    /// <summary>
    /// Asynchronously waits for transactions to be available.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous flush operation. The task result is true if operations were flushed, otherwise false.</returns>
    public ValueTask<bool> WaitAsync(CancellationToken cancellationToken)
    {
        return _pendingTransactions.WaitAsync(cancellationToken);
    }

    public void FlushToDisk() => RandomAccess.FlushToDisk(_file);

    public unsafe void WriteToDisk()
    {
        var size = Unsafe.SizeOf<JournalFrame>();
        using var block = AlignedMemory.Allocate(4096, 4096);

        var bytesLeft = 4096;
        var offset = 0;
        var pendingCommits = new List<Transaction>();

        while (_pendingTransactions.TryPeek(out var transaction))
        {
            var frame = new JournalFrame
            {
                TransactionId = ++_transactionId,
                Timestamp = DateTime.Now.Ticks
            };

            bytesLeft -= size + transaction.Data.Length;

            if (bytesLeft < 0)
                break;

            Unsafe.Write((byte*)block.Pointer + offset, frame);

            offset += size + transaction.Data.Length;

            _pendingTransactions.TryDequeue(out _);
            pendingCommits.Add(transaction);
        }

        RandomAccess.Write(_file, block.Span, _offset);

        _offset += 4096;

        foreach (var transaction in pendingCommits)
            transaction.Commit();
    }
}