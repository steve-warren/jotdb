using System.Diagnostics;
using System.Threading.Channels;

namespace JotDB.Storage.Documents;

public class DocumentCollectionFileBuffer
{
    private readonly Channel<DocumentOperation> _channel;

    public DocumentCollectionFileBuffer()
    {
        _channel = Channel.CreateUnbounded<DocumentOperation>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = true
        });
    }

    public void Close() => _channel.Writer.Complete();

    public void Write(
        ReadOnlySpan<DocumentOperation> documentOperations)
    {
        foreach (var documentOperation in documentOperations)
            _ = _channel.Writer.TryWrite(documentOperation);

        Debug.WriteLine($"written {documentOperations.Length} data entries to the data pipe.");
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