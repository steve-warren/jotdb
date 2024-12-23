using JotDB.Storage;

namespace JotDB.Metrics;

public static class MetricSink
{
    public static class StorageTransactions
    {
        private static ExponentialMovingAverage _mergedTransactionCount = new();
        private static ExponentialMovingAverage _executionTime = new();
        private static ExponentialMovingAverage _bytesCommitted = new();

        public static void Apply(StorageTransaction transaction)
        {
            _mergedTransactionCount.Update(transaction.MergedTransactionCount);
            _executionTime.Update(transaction.ExecutionTime.Ticks);
            _bytesCommitted.Update(transaction.BytesCommitted);
        }

        public static long MergedTransactionCount => _mergedTransactionCount.ReadLong();
        public static TimeSpan ExecutionTime => _executionTime.ReadTimeSpan();
        public static long BytesCommitted => _bytesCommitted.ReadLong();
    }
}