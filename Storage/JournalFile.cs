using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using Microsoft.Win32.SafeHandles;

namespace JotDB.Storage;

public sealed class JournalFile : IDisposable
{
    private const int JOURNAL_FLUSH_BUFFER_SIZE = 8;
    private const int JOURNAL_MEMORY_BUFFER_SIZE = 128;

    private readonly Channel<DocumentOperation> _inboundBuffer;
    private readonly Channel<DocumentOperation> _outboundBuffer;
    private readonly DocumentOperation[] _flushBuffer;

    private readonly SafeFileHandle _file;
    private ulong _identity;
    private long _offset;

    public static JournalFile Open(
        string path)
    {
        return new JournalFile(
            path: path,
            offset: 0);
    }

    private JournalFile(
        string path,
        long offset)
    {
        Path = path;
        _offset = offset;

        _file = File.OpenHandle(
            path: path,
            mode: FileMode.OpenOrCreate,
            access: FileAccess.ReadWrite,
            share: FileShare.None,
            options: FileOptions.WriteThrough | (FileOptions)0x20000000);

        _inboundBuffer = Channel.CreateBounded<DocumentOperation>(
            new BoundedChannelOptions(JOURNAL_MEMORY_BUFFER_SIZE)
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = true,
                FullMode = BoundedChannelFullMode.Wait
            });

        _outboundBuffer = Channel.CreateUnbounded<DocumentOperation>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true,
                AllowSynchronousContinuations = true,
            });

        _flushBuffer = new DocumentOperation[JOURNAL_FLUSH_BUFFER_SIZE];
    }

    public string Path { get; }

    public void Close() => _inboundBuffer.Writer.TryComplete();

    public void Dispose()
    {
        _inboundBuffer.Writer.TryComplete();
        _file.Dispose();
    }

    public async IAsyncEnumerable<DocumentOperation> WaitToReadAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (await _outboundBuffer.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        while (_outboundBuffer.Reader.TryRead(out var document))
            yield return document;
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

        await _inboundBuffer.Writer
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
        if (!await _inboundBuffer.Reader.WaitToReadAsync(cancellationToken))
            return false;

        var reader = _inboundBuffer.Reader;
        var i = 0;

        var bufferLimit = 4096;

        for (; i < _flushBuffer.Length; i++)
        {
            if (!reader.TryRead(out var operation)) break;

            bufferLimit -= operation.Data.Length;

            if (bufferLimit < 0)
                break;

            _flushBuffer[i] = operation;
        }

        var memory = _flushBuffer.AsMemory(start: 0, length: i);

        WriteToFile(memory);

        for (i = 0; i < memory.Length; i++)
            _outboundBuffer.Writer.TryWrite(_flushBuffer[i]);

        return true;
    }

    private unsafe void WriteToFile(
        ReadOnlyMemory<DocumentOperation> documentOperations)
    {
        const nuint alignment = 4096;
        const nuint bufferSize = 4096;

        var alignedBuffer = NativeMemory.AlignedAlloc(bufferSize, alignment);
        Exception? ioException = null;

        if (alignedBuffer == null)
        {
            Debugger.Break();
            throw new OutOfMemoryException("failed to allocate aligned memory.");
        }

        try
        {
            var span = new Span<byte>(alignedBuffer, (int)bufferSize);
            span.Clear();

            var offset = 0;
            
            for (var i = 0; i < documentOperations.Length; i++)
            {
                var documentOperation = documentOperations.Span[i];

                documentOperation.AssignOperationId(++_identity);

                documentOperation.Data.Span.CopyTo(span.Slice(offset, documentOperation.Data.Length));
                offset += documentOperation.Data.Length;
            }
            
            RandomAccess.Write(_file, new ReadOnlySpan<byte>(alignedBuffer, (int)bufferSize), _offset);
            _offset += 4096;
        }

        catch (IOException ex)
        {
            ioException = ex;
        }

        finally
        {
            NativeMemory.AlignedFree(alignedBuffer);

            for (var i = 0; i < documentOperations.Length; i++)
            {
                var entry = documentOperations.Span[i];
                entry.FlushJournal(ioException);
            }
        }
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