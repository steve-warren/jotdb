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
    public DatabaseCommand(uint operationSequenceNumber,
        ulong transactionSequenceNumber,
        ReadOnlyMemory<byte> data,
        DatabaseOperationType type)
    {
        OperationSequenceNumber = operationSequenceNumber;
        TransactionSequenceNumber = transactionSequenceNumber;
        Data = data;
        Type = type;
    }

    public DatabaseCommandStatus Status { get; private set; } = DatabaseCommandStatus.Created;
    public TimeSpan ExecutionTime { get; private set; }
    public uint OperationSequenceNumber { get; }
    public ulong TransactionSequenceNumber { get; }
    public ReadOnlyMemory<byte> Data { get; }
    public DatabaseOperationType Type { get; }

    public void Execute()
    {
        Status = DatabaseCommandStatus.Executing;
        var executionTime = StopwatchSlim.StartNew();
        Console.WriteLine(
            $"Executing operation {Type}: OSN: {OperationSequenceNumber} TSN: {TransactionSequenceNumber}");
        ExecutionTime = executionTime.Elapsed;
        Status = DatabaseCommandStatus.Executed;
    }
}