﻿using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using JotDB.Memory;
using JotDB.Threading;

namespace JotDB.Storage.Journal;

/// <summary>
/// Represents a wrapper around a transaction, designed to work with a write-ahead log (WAL) mechanism.
/// </summary>
/// <remarks>
/// This class encapsulates a transaction and provides mechanisms to manage its state and commit handling
/// within the context of a write-ahead log. The transaction can wait for commit signals, be aborted with an exception,
/// or commit after the completion of a specific task.
/// </remarks>
public sealed class WriteAheadLogTransaction
{
    private readonly AsyncAwaiter _awaiter = new();

    /// <summary>
    /// Represents a wrapper around a transaction, designed to work with a write-ahead log (WAL) mechanism.
    /// </summary>
    /// <remarks>
    /// This class encapsulates a transaction and provides mechanisms to manage its state and commit handling
    /// within the context of a write-ahead log. The transaction can wait for commit signals, be aborted with an exception,
    /// or commit after the completion of a specific task.
    /// </remarks>
    public WriteAheadLogTransaction(DatabaseTransaction databaseTransaction)
    {
        DatabaseTransaction = databaseTransaction;
        Size = (uint)WriteAheadLogTransactionHeader.Size *
               databaseTransaction.OperationCount +
               databaseTransaction
                   .Size;
    }

    public uint Size { get; }

    public DatabaseTransaction DatabaseTransaction { get; }
    public uint CommitSequenceNumber { get; private set; }

    public Task WaitUntilCommitAsync()
    {
        return _awaiter.WaitForSignalAsync();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryWriteToMemory(
        ref AlignedMemoryWriter writer,
        uint commitSequenceNumber,
        long timestamp)
    {
        if (writer.BytesAvailable < Size)
            return false;

        CommitSequenceNumber = commitSequenceNumber;

        var header = new WriteAheadLogTransactionHeader
        {
            TransactionSequenceNumber =
                DatabaseTransaction.TransactionSequenceNumber,
            CommitSequenceNumber = commitSequenceNumber,
            TransactionType = (int)DatabaseTransaction.Type,
            Timestamp = timestamp
        };

        foreach (var operation in DatabaseTransaction.Operations)
        {
            var span = operation.Data.Span;
            header.DataLength = operation.Data.Length;
            header.Hash = MD5.HashData(span);
            writer.Write(header);
            writer.Write(span);
        }

        return true;
    }

    public void Abort(Exception ex)
    {
        _awaiter.SignalFault(ex);
    }

    public void Commit(Task after)
    {
        _awaiter.SignalCompletionAfter(after);
    }
}