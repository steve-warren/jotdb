namespace JotDB.Metrics;

public struct Counter
{
    private ulong _count;

    public void Increment()
    {
        Interlocked.Increment(ref _count);
    }

    public ulong Count => Volatile.Read(ref _count);
}