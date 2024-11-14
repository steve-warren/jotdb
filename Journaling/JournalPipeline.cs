using System.Threading.Channels;

namespace JotDB;

public sealed class JournalPipeline
{
    private readonly Channel<JournalEntry> _channel =
        Channel.CreateBounded<JournalEntry>(new BoundedChannelOptions(5)
    {
        SingleReader = true,
        SingleWriter = false,
        AllowSynchronousContinuations = true,
        FullMode = BoundedChannelFullMode.Wait
    });

    public async Task<JournalEntry> SendAsync(
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