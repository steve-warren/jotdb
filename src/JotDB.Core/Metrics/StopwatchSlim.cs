using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace JotDB.Metrics;

public readonly ref struct StopwatchSlim
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static StopwatchSlim StartNew() => new();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public StopwatchSlim()
    {
        Timestamp = Stopwatch.GetTimestamp();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public StopwatchSlim(long timestamp)
    {
        Timestamp = timestamp;
    }

    public long Timestamp
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get;
    }

    public TimeSpan Elapsed
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => Stopwatch.GetElapsedTime(Timestamp);
    }
}