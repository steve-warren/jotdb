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
    private readonly PageCollection _pageCollection;

    public DatabaseCommand(
        PageCollection pageCollection,
        uint commandSequenceNumber,
        ulong transactionSequenceNumber,
        ReadOnlyMemory<byte> data,
        DatabaseCommandType commandType)
    {
        _pageCollection = pageCollection;
        CommandSequenceNumber = commandSequenceNumber;
        TransactionSequenceNumber = transactionSequenceNumber;
        Data = data;
        CommandType = commandType;
    }

    public DatabaseCommandStatus CommandStatus { get; private set; } =
        DatabaseCommandStatus.Created;

    public TimeSpan ExecutionTime { get; private set; }
    public uint CommandSequenceNumber { get; }
    public ulong TransactionSequenceNumber { get; }
    public ReadOnlyMemory<byte> Data { get; }
    public DatabaseCommandType CommandType { get; }

    public void Execute()
    {
        CommandStatus = DatabaseCommandStatus.Executing;
        var executionTime = StopwatchSlim.StartNew();

        DatabaseCommandExecutor.Execute(
            _pageCollection,
            this);

        ExecutionTime = executionTime.Elapsed;
        CommandStatus = DatabaseCommandStatus.Executed;

        MetricSink.DatabaseCommands.Apply(this);
    }
}