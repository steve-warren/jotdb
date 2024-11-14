using System.Diagnostics;
using System.Threading.Channels;

namespace JotDB.Storage.Data;

public class DataPipe
{
    private readonly Channel<DocumentOperation> _channel;

    public DataPipe()
    {
        _channel = Channel.CreateUnbounded<DocumentOperation>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = false
        });
    }

    public void Send(ReadOnlySpan<DocumentOperation> journalEntry)
    {
        foreach(var entry in journalEntry)
            _ = _channel.Writer.TryWrite(entry);

        Debug.WriteLine($"written {journalEntry.Length} data entries to the data pipe.");
    }
    
    public async Task WaitAndReceiveAsync(
        Memory<DocumentOperation> buffer,
        CancellationToken cancellationToken)
    {
        var reader = _channel.Reader;
    }
}