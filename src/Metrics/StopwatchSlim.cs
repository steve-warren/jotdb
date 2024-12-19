using System.Diagnostics;

namespace JotDB.Metrics;

public readonly ref struct StopwatchSlim
{
    public static StopwatchSlim StartNew() => new();

    public StopwatchSlim()
    {
        Timestamp = Stopwatch.GetTimestamp();
    }

    public StopwatchSlim(long timestamp)
    {
        Timestamp = timestamp;
    }

    public long Timestamp { get; }
    public TimeSpan Elapsed => Stopwatch.GetElapsedTime(Timestamp);
}