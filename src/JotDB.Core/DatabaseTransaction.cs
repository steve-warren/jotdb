using System.Diagnostics;
using JotDB.Metrics;
using JotDB.Storage;
using JotDB.Storage.Documents;
using JotDB.Storage.Journal;

namespace JotDB;

/// <summary>
/// Represents a transactional operation within the database system.
/// </summary>
/// <remarks>
/// A transaction encapsulates a series of operations that can be committed or rolled back as a single unit.
/// Transactions are uniquely identified by their sequence numbers and can store associated data.
/// </remarks>
public sealed class DatabaseTransaction
{
    private readonly PageCollection _pageCollection;
    private readonly WriteAheadLog _wal;
    private uint _commandSequenceNumber = 0;

    public DatabaseTransaction(
        PageCollection pageCollection,
        WriteAheadLog wal)
    {
        _pageCollection = pageCollection;
        _wal = wal;
    }

    public int Timeout { get; set; } = -1;
    public ulong TransactionSequenceNumber { get; init; }
    public TransactionType Type { get; set; }
    public TimeSpan ExecutionTime { get; private set; }

    /// <summary>
    /// The total number of uncompressed bytes to be written to disk.
    /// </summary>
    public uint Size { get; private set; }
    public uint CommandCount { get; private set; }
    public List<DatabaseCommand> Commands { get; } = [];

    public void EnlistCommand(
        DatabaseCommandType type,
        ReadOnlyMemory<byte> data)
    {
        // snapshot isolation: each transaction has
        // its own snapshot of the entire database

        var command = new DatabaseCommand(
            _pageCollection,
            ++_commandSequenceNumber,
            TransactionSequenceNumber,
            data,
            type);

        Commands.Add(command);

        Size += (uint)data.Length;
        CommandCount++;
    }

    public async Task CommitAsync()
    {
        var commitTime = Stopwatch.StartNew();

        foreach (var command in Commands)
            command.Execute();

        // wait for the WAL thread to write the transaction to disk.
        await _wal.AppendAsync(this).ConfigureAwait
            (false);

        ExecutionTime = commitTime.Elapsed;

        MetricSink.DatabaseTransactions.Apply(this);
    }

    public Task AbortAsync()
    {
        throw new NotImplementedException();
    }
}