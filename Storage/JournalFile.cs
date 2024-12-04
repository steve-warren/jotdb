using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using Microsoft.Win32.SafeHandles;

namespace JotDB.Storage;

[Flags]
public enum JournalEntryOptions : byte
{
    None = 0
}

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct JournalEntryHeader
{
    public uint TransactionId;
    public long Timestamp;
    public JournalEntryOptions Flags;
    public ulong PageOffset;
}

public sealed class JournalFile : IDisposable
{
    private const int JOURNAL_FLUSH_BUFFER_SIZE = 8;
    private const int JOURNAL_MEMORY_BUFFER_SIZE = 128;

    private readonly Channel<JournalEntry> _inboundBuffer;
    private readonly JournalEntry[] _flushBuffer;

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

        _inboundBuffer = Channel.CreateBounded<JournalEntry>(
            new BoundedChannelOptions(JOURNAL_MEMORY_BUFFER_SIZE)
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = true,
                FullMode = BoundedChannelFullMode.Wait
            });

        _flushBuffer = new JournalEntry[JOURNAL_FLUSH_BUFFER_SIZE];
    }

    public string Path { get; }

    public void Close() => _inboundBuffer.Writer.TryComplete();

    public void Dispose()
    {
        _inboundBuffer.Writer.TryComplete();
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
        JournalEntryType entryType)
    {
        var entry = new JournalEntry
        {
            Data = data,
            OperationType = entryType
        };

        return Task.WhenAll(
            _inboundBuffer.Writer.WriteAsync(entry).AsTask(),
            entry.WaitForCommitAsync(CancellationToken.None)
        );
    }

    /// <summary>
    /// Asynchronously waits for operations to be available and flushes them to the journal file.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous flush operation. The task result is true if operations were flushed, otherwise false.</returns>
    public async Task WaitToFlushAsync(CancellationToken cancellationToken)
    {
        var reader = _inboundBuffer.Reader;

        // we can safely cancel here because we're only waiting for the channel to be drained.
        if (!await reader.WaitToReadAsync(cancellationToken))
            return;

        using var block = AlignedMemory.Allocate(4096, 4096);

        reader.TryRead(out var item);

        item.Commit();
    }
}