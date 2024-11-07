using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading.Channels;

namespace JotDB;

public class Journal
{
    private ulong _journalIdentitySeed;
    private readonly AppendOnlyFile _file;
    private readonly Channel<JournalEntry> _channel;

    public Journal(ulong journalIdentitySeed, AppendOnlyFile file)
    {
        _journalIdentitySeed = journalIdentitySeed;
        _file = file;
        _channel = Channel.CreateBounded<JournalEntry>(new BoundedChannelOptions(5)
        {
            AllowSynchronousContinuations = true,
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });
    }

    /// <summary>
    /// Asynchronously writes a new journal entry with the given data.
    /// </summary>
    /// <param name="data">The data to be written in the journal.</param>
    /// <returns>A task that represents the asynchronous write operation.</returns>
    public async Task WriteJournalEntryAsync(ReadOnlyMemory<byte> data)
    {
        var identity = Interlocked.Increment(ref _journalIdentitySeed);
        var entry = new JournalEntry(identity, data);

        await _channel.Writer.WriteAsync(entry).ConfigureAwait(false);

        await entry.WriteCompletionTask;
    }

    public async Task ProcessJournalEntriesAsync(CancellationToken cancellationToken)
    {
        var reader = _channel.Reader;
        var completionQueue = new Queue<JournalEntry>();
        var watch = new Stopwatch();

        while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (reader.TryRead(out var entry))
            {
                WriteJournalEntry(entry);

                completionQueue.Enqueue(entry);
            }

            watch.Restart();
            _file.Flush();

            Console.WriteLine($"Flushed {completionQueue.Count} journal entries to disk in {watch.ElapsedMilliseconds} ms.");

            while (completionQueue.TryDequeue(out var entry))
                entry.FinishWriting();
        }
    }

    private void WriteJournalEntry(JournalEntry entry)
    {
        Span<byte> buffer = stackalloc byte[12]; // ulong + int32

        MemoryMarshal.Write(buffer[..8], entry.Identity);
        MemoryMarshal.Write(buffer[8..], entry.Data.Length);

        _file.Write(buffer);
        _file.Write(entry.Data.Span);
    }
}