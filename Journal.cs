using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading.Channels;

namespace JotDB;

public enum DatabaseOperation : byte
{
    Insert = 1,
    Update = 2,
    Delete = 3
}

public sealed class Journal : IAsyncDisposable
{
    private ulong _journalIdentitySeed;
    private readonly AppendOnlyFile _file;
    private readonly Channel<JournalEntry> _pendingJournalWrites;
    private readonly ChannelWriter<JournalEntry> _pendingDocumentCollectionWrites;

    public Journal(ulong journalIdentitySeed,
        string path,
        ChannelWriter<JournalEntry> pendingDocumentCollectionWrites)
    {
        _journalIdentitySeed = journalIdentitySeed;
        _file = new AppendOnlyFile(path);

        _pendingJournalWrites = Channel.CreateBounded<JournalEntry>(new BoundedChannelOptions(5)
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = true,
            FullMode = BoundedChannelFullMode.Wait
        });

        _pendingDocumentCollectionWrites = pendingDocumentCollectionWrites;
    }

    /// <summary>
    /// Asynchronously writes a new journal entry with the given data.
    /// </summary>
    /// <param name="data">The data to be written in the journal.</param>
    /// <param name="operation"></param>
    /// <returns>A task that represents the asynchronous write operation.</returns>
    public async Task<JournalEntry> WriteJournalEntryAsync(
        ReadOnlyMemory<byte> data,
        DatabaseOperation operation)
    {
        var entry = new JournalEntry
        {
            Data = data,
            Operation = operation
        };

        await _pendingJournalWrites.Writer.WriteAsync(entry).ConfigureAwait(false);

        await entry.WriteCompletionTask.ConfigureAwait(false);

        return entry;
    }

    public async Task ProcessJournalEntriesAsync(CancellationToken cancellationToken)
    {
        var reader = _pendingJournalWrites.Reader;
        var completionQueue = new Queue<JournalEntry>();
        var dataFileQueue = new Queue<JournalEntry>();
        var watch = new Stopwatch();

        // blocks until signaled that the channel has data
        while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            watch.Restart();

            // read all the data available
            while (reader.TryRead(out var entry))
            {
                entry.Identity = Interlocked.Increment(ref _journalIdentitySeed);

                // write to the file
                WriteJournalEntry(entry);

                // track the journal entry for signaling when the os buffer is flushed to disk
                completionQueue.Enqueue(entry);
            }

            // flush all writes from the os buffer to disk
            _file.Flush();

            Debug.WriteLine($"Flushed {completionQueue.Count} journal entries to disk in {watch.ElapsedMilliseconds} ms.");

            // mark all journal entries as written to disk
            while (completionQueue.TryDequeue(out var entry))
            {
                entry.FinishWriting();
                dataFileQueue.Enqueue(entry);
            }

            while (dataFileQueue.TryDequeue(out var entry))
                await _pendingDocumentCollectionWrites.WriteAsync(entry).ConfigureAwait(false);
        }
    }

    private void WriteJournalEntry(JournalEntry entry)
    {
        Span<byte> buffer = stackalloc byte[13]; // ulong + int32 + byte

        MemoryMarshal.Write(buffer[..8], entry.Identity);
        MemoryMarshal.Write(buffer[8..12], entry.Data.Length);
        MemoryMarshal.Write(buffer[12..], entry.Operation);

        _file.Write(buffer);
        _file.Write(entry.Data.Span);
    }

    public ValueTask DisposeAsync()
    {
        return _file.DisposeAsync();
    }
}