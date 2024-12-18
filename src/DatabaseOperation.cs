namespace JotDB;

public sealed record DatabaseOperation(
    uint OperationSequenceNumber,
    ulong TransactionSequenceNumber,
    ReadOnlyMemory<byte> Data,
    DatabaseOperationType Type);