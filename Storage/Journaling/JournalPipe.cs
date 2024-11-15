using System.Threading.Channels;

namespace JotDB.Storage.Journaling;

/// <summary>
/// Handles asynchronous operations for journaling document changes using a channel for communication.
/// </summary>
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

    public async ValueTask<DocumentOperation> WriteAsync(
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

    public ValueTask<bool> WaitToReadAsync(
        CancellationToken cancellationToken)
    {
        return _channel.Reader.WaitToReadAsync(cancellationToken);
    }

    public int Read(
        Span<DocumentOperation> buffer)
    {
        var reader = _channel.Reader;

        for (var i = 0; i < buffer.Length; i++)
        {
            if (!reader.TryRead(out var entry)) return i;
            buffer[i] = entry;
        }

        return buffer.Length;
    }
}