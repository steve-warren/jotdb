using JotDB.Storage;

namespace JotDB.Metrics;

public static class MetricSink
{
    public static class DatabaseTransactions
    {
        private static Counter _transactionCount = new();
        private static ExponentialMovingAverage _transactionExecutionTime =
            new();

        public static TimeSpan AverageTransactionExecutionTime =>
            _transactionExecutionTime.ReadTimeSpan();

        public static ulong TransactionCount => _transactionCount.Count;

        public static void Apply(DatabaseTransaction transaction)
        {
            _transactionExecutionTime.Update(transaction.ExecutionTime.Ticks);
            _transactionCount.Increment();
        }
    }

    public static class StorageTransactions
    {
        private static ExponentialMovingAverage _mergedTransactionCount = new();
        private static ExponentialMovingAverage _executionTime = new();
        private static ExponentialMovingAverage _bytesCommitted = new();

        public static void Apply(StorageTransaction transaction)
        {
            _mergedTransactionCount.Update(transaction.TransactionMergeCount);
            _executionTime.Update(transaction.ExecutionTime.Ticks);
            _bytesCommitted.Update(transaction.BytesCommitted);
        }

        public static long AverageMergedTransactionCount =>
            _mergedTransactionCount.ReadLong();

        public static TimeSpan AverageExecutionTime =>
            _executionTime.ReadTimeSpan();

        public static long AverageBytesCommitted => _bytesCommitted.ReadLong();
    }

    public static class WriteAheadLog
    {
        private static ExponentialMovingAverage _rotationTime = new();
        private static ExponentialMovingAverage _writeTime = new();
        private static Counter _rotationCount;
        private static Counter _writeCount;

        public static void Rotate(TimeSpan time)
        {
            _rotationTime.Update(time.Ticks);
            _rotationCount.Increment();
        }

        public static void Write(TimeSpan time)
        {
            _writeTime.Update(time.Ticks);
            _writeCount.Increment();
        }

        public static TimeSpan AverageRotationTime =>
            _rotationTime.ReadTimeSpan();

        public static TimeSpan AverageWriteTime => _writeTime
            .ReadTimeSpan();

        public static ulong RotationCount => _rotationCount.Count;
        public static ulong WriteCount => _writeCount.Count;
    }
}