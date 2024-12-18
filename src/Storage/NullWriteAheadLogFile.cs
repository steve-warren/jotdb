using JotDB.Memory;

namespace JotDB.Storage;

public class NullWriteAheadLogFile(
    double flushTimeoutInMilliseconds = 50,
    double writeTimeoutInMilliseconds = 1) : IWriteAheadLogFile
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