using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using Microsoft.Win32.SafeHandles;

namespace JotDB.Storage;

public sealed class JournalFile : IDisposable
{
    private const int JOURNAL_FLUSH_BUFFER_SIZE = 8;
    private const int JOURNAL_MEMORY_BUFFER_SIZE = 128;

    private readonly Channel<DocumentOperation> _pendingWriteBuffer;
    private readonly DocumentOperation[] _flushBuffer;

    private readonly SafeFileHandle _file;
    private ulong _identity;
    private long _offset;

    public static JournalFile Open(
        string path)
    {
        using var file = File.Open(path, new FileStreamOptions
        {
            Access = FileAccess.Read,
            Mode = FileMode.OpenOrCreate,
            Share = FileShare.ReadWrite
        });

        return new JournalFile(
            path: path,
            offset: file.Length);
    }

    private JournalFile(
        string path,
        long offset)
    {
        Path = path;
        _offset = offset;

        _file = File.OpenHandle(
            path: path,
            mode: FileMode.Append,
            access: FileAccess.Write,
            share: FileShare.Read);

        _pendingWriteBuffer = Channel.CreateBounded<DocumentOperation>(new BoundedChannelOptions(JOURNAL_MEMORY_BUFFER_SIZE)
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = true,
            FullMode = BoundedChannelFullMode.Wait
        });
        _flushBuffer = new DocumentOperation[JOURNAL_FLUSH_BUFFER_SIZE];
    }

    public string Path { get; }

    public void Close() => _pendingWriteBuffer.Writer.Complete();

    public void Dispose()
    {
        _pendingWriteBuffer.Writer.Complete();
        _file.Dispose();
    }

    /// <summary>
    /// Asynchronously writes a document operation to the journal file.
    /// </summary>
    /// <param name="data">The data to be written to the journal file.</param>
    /// <param name="operationType">The type of document operation (Insert, Update, Delete).</param>
    /// <returns>A task that represents the asynchronous write operation. The task result contains the unique operation ID of the written operation.</returns>
    public async Task<ulong> WriteAsync(
        ReadOnlyMemory<byte> data,
        DocumentOperationType operationType)
    {
        var operation = new DocumentOperation
        {
            Data = data,
            OperationType = operationType
        };

        await _pendingWriteBuffer.Writer
            .WriteAsync(operation)
            .ConfigureAwait(false);

        await operation
            .WaitForJournalFlushAsync(CancellationToken.None)
            .ConfigureAwait(false);

        return operation.OperationId;
    }

    /// <summary>
    /// Asynchronously waits for operations to be available and flushes them to the journal file.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous flush operation. The task result is true if operations were flushed, otherwise false.</returns>
    public async Task<bool> WaitToFlushAsync(CancellationToken cancellationToken)
    {
        // we can safely cancel here because we're only waiting for the channel to be drained.
        if (!await _pendingWriteBuffer.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            return false;

        var reader = _pendingWriteBuffer.Reader;
        var i = 0;

        for (; i < _flushBuffer.Length; i++)
        {
            if (!reader.TryRead(out var operation)) break;
            _flushBuffer[i] = operation;
        }

        var span = _flushBuffer.AsSpan(start: 0, length: i);

        WriteToFile(span);

        return true;
    }

    private void WriteToFile(
        ReadOnlySpan<DocumentOperation> documentOperations)
    {
        var buffers = new ReadOnlyMemory<byte>[documentOperations.Length * 2];
        var rented = new List<byte[]>();
        var j = 0;

        for (var i = 0; i < documentOperations.Length; i++, j++)
        {
            var documentOperation = documentOperations[i];

            documentOperation.AssignOperationId(++_identity);

            var buffer = ArrayPool<byte>.Shared.Rent(13);
            rented.Add(buffer);

            var span = buffer.AsSpan(0, 13);

            SerializeJournalEntry(documentOperation, span);

            buffers[j] = buffer.AsMemory(0, 13);
            j++;
            buffers[j] = documentOperation.Data;
        }

        RandomAccess.Write(_file, buffers, _offset);

        foreach (var entry in documentOperations)
        {
            entry.FlushJournal();
            _offset += 13 + entry.Data.Length;
        }

        foreach (var buffer in rented)
            ArrayPool<byte>.Shared.Return(buffer);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void SerializeJournalEntry(
        DocumentOperation entry,
        Span<byte> buffer)
    {
        MemoryMarshal.Write(buffer[..8], entry.OperationId);
        MemoryMarshal.Write(buffer[8..12], entry.Data.Length);
        MemoryMarshal.Write(buffer[12..], entry.OperationType);
    }
}