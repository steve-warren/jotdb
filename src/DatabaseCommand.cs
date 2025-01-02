using JotDB.Metrics;
using JotDB.Storage.Documents;

namespace JotDB;

public enum DatabaseCommandStatus
{
    Created,
    Executing,
    Executed
}

public sealed class DatabaseCommand
{
    private readonly PageCollection _pages;

    public DatabaseCommand(
        PageCollection pages,
        uint commandSequenceNumber,
        ulong transactionSequenceNumber,
        ReadOnlyMemory<byte> data,
        DatabaseOperationType type)
    {
        _pages = pages;
        CommandSequenceNumber = commandSequenceNumber;
        TransactionSequenceNumber = transactionSequenceNumber;
        Data = data;
        Type = type;
    }

    public DatabaseCommandStatus Status { get; private set; } = DatabaseCommandStatus.Created;
    public TimeSpan ExecutionTime { get; private set; }
    public uint CommandSequenceNumber { get; }
    public ulong TransactionSequenceNumber { get; }
    public ReadOnlyMemory<byte> Data { get; }
    public DatabaseOperationType Type { get; }

    public void Execute()
    {
        Status = DatabaseCommandStatus.Executing;
        var executionTime = StopwatchSlim.StartNew();

        using var page = _pages.Allocate();
        
        page.Write(Data.Span);
        
        ExecutionTime = executionTime.Elapsed;
        Status = DatabaseCommandStatus.Executed;
    }
}