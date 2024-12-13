using System.Threading.Channels;

namespace JotDB.Storage;

public sealed class TransactionBuffer : IDisposable
{
    private readonly Channel<Transaction> _channel = Channel.CreateBounded<Transaction>(
        new BoundedChannelOptions(Environment.ProcessorCount * 2)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });

    public ValueTask<bool> WaitForTransactionsAsync(CancellationToken cancellationToken = default) =>
        _channel.Reader.WaitToReadAsync(cancellationToken);

    public IEnumerable<Transaction> ReadTransactions()
    {
        while (_channel.Reader.TryRead(out var transaction))
            yield return transaction;
    }

    public ValueTask WriteTransactionAsync(Transaction transaction) =>
        _channel.Writer.WriteAsync(transaction);

    public void Dispose() =>
        _channel.Writer.TryComplete();
}