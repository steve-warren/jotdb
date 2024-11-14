using System.Threading.Channels;

namespace JotDB;

public sealed class JournalPipeline
{
    private readonly Channel<JournalEntry> _channel =
        Channel.CreateBounded<JournalEntry>(new BoundedChannelOptions(128)
    {
        SingleReader = true,
        SingleWriter = false,
        AllowSynchronousContinuations = true,
        FullMode = BoundedChannelFullMode.Wait
    });

    public async ValueTask<JournalEntry> SendAsync(
        ReadOnlyMemory<byte> data,
        DatabaseOperation operation,
        CancellationToken cancellationToken)
    {
        var entry = new JournalEntry
        {
            Data = data,
            Operation = operation
        };

        await _channel.Writer
            .WriteAsync(entry, cancellationToken)
            .ConfigureAwait(false);

        return entry;
    }

    /// <summary>
    /// Waits for and receives journal entries asynchronously into the provided buffer.
    /// </summary>
    /// <param name="buffer">The memory buffer where the received journal entries will be stored.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the operation.</param>
    /// <returns>The number of journal entries received and stored into the buffer.</returns>
    public async Task<int> WaitAndReceiveAsync(
        Memory<JournalEntry> buffer,
        CancellationToken cancellationToken)
    {
        var reader = _channel.Reader;

        if (!await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false)) return 0;

        var span = buffer.Span;

        for (var i = 0; i < span.Length; i++)
        {
            if (!reader.TryRead(out var entry)) return i;
            span[i] = entry;
        }

        return span.Length;
    }
}