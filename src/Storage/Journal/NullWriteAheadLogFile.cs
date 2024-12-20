using JotDB.Memory;

namespace JotDB.Storage.Journal;

public class NullWriteAheadLogFile(
    double flushTimeoutInMilliseconds = 50,
    double writeTimeoutInMilliseconds = 0.02) : IWriteAheadLogFile
{
    public void FlushToDisk()
    {
        Thread.Sleep(TimeSpan.FromMilliseconds(flushTimeoutInMilliseconds));
    }

    public void WriteToDisk(AlignedMemory memory)
    {
        Thread.Sleep(TimeSpan.FromMilliseconds(writeTimeoutInMilliseconds));
    }
}