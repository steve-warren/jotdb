using System.Threading.Channels;

namespace JotDB.Storage.Journaling;

public sealed class JournalFileBuffer
{
    private readonly Channel<DocumentOperation> _channel =
        Channel.CreateBounded<DocumentOperation>(new BoundedChannelOptions(128)
    {
        SingleReader = true,
        SingleWriter = false,
        AllowSynchronousContinuations = true,
        FullMode = BoundedChannelFullMode.Wait
    });

    public void Close()
    {
        _channel.Writer.Complete();
    }

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
        CancellationToken cancellationToken) =>
        _channel.Reader.WaitToReadAsync(cancellationToken);

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