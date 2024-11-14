using System.Threading.Channels;

namespace JotDB.Storage.Journaling;

public sealed class JournalPipe
{
    private readonly Channel<DocumentOperation> _channel =
        Channel.CreateBounded<DocumentOperation>(new BoundedChannelOptions(128)
    {
        SingleReader = true,
        SingleWriter = false,
        AllowSynchronousContinuations = true,
        FullMode = BoundedChannelFullMode.Wait
    });

    public async ValueTask<DocumentOperation> SendAsync(
        ReadOnlyMemory<byte> data,
        DocumentOperationType operationType,
        CancellationToken cancellationToken)
    {
        var operation = new DocumentOperation
        {
            Data = data,
            OperationType = operationType
        };

        await _channel.Writer
            .WriteAsync(operation, cancellationToken)
            .ConfigureAwait(false);

        return operation;
    }

    /// <summary>
    /// Waits for and receives journal entries asynchronously into the provided buffer.
    /// </summary>
    /// <param name="buffer">The memory buffer where the received journal entries will be stored.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the operation.</param>
    /// <returns>The number of journal entries received and stored into the buffer.</returns>
    public async Task<int> WaitAndReceiveAsync(
        Memory<DocumentOperation> buffer,
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