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
    private readonly PageCollection _pages;
    private readonly WriteAheadLog _wal;
    private uint _commandSequenceNumber = 0;

    public DatabaseTransaction(
        PageCollection pages,
        WriteAheadLog wal)
    {
        _pages = pages;
        _wal = wal;
    }

    public int Timeout { get; set; } = -1;
    public ulong TransactionSequenceNumber { get; init; }
    public TransactionType Type { get; set; }
    public TimeSpan ExecutionTime { get; private set; }
    public uint Size { get; private set; }
    public uint CommandCount { get; private set; }
    public List<DatabaseCommand> Commands { get; } = [];

    public DatabaseCommand CreateCommand(
        DatabaseOperationType type,
        ReadOnlyMemory<byte> data)
    {
        // snapshot isolation: each transaction has
        // its own snapshot of the entire database

        var command = new DatabaseCommand(
            ++_commandSequenceNumber,
            TransactionSequenceNumber,
            data,
            type);

        Commands.Add(command);

        Size += (uint)data.Length;
        CommandCount++;

        return command;
    }

    public async Task CommitAsync()
    {
        var commitTime = Stopwatch.StartNew();

        // wait for the WAL thread to write the transaction to disk.
        await _wal.AppendAsync(this).ConfigureAwait
            (false);

        ExecutionTime = commitTime.Elapsed;

        MetricSink.DatabaseTransactions.Apply(this);
    }
}