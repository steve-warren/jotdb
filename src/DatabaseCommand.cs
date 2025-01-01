using JotDB.Metrics;

namespace JotDB;

public enum DatabaseCommandStatus
{
    Created,
    Executing,
    Executed
}

public sealed class DatabaseCommand
{
    public DatabaseCommand(uint commandSequenceNumber,
        ulong transactionSequenceNumber,
        ReadOnlyMemory<byte> data,
        DatabaseOperationType type)
    {
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
        ExecutionTime = executionTime.Elapsed;
        Status = DatabaseCommandStatus.Executed;
    }
}