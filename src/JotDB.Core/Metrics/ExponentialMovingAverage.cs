namespace JotDB.Metrics;

public struct ExponentialMovingAverage
{
    private readonly double _alpha = 0.2;

    // The first few EMA values are slightly influenced by the chosen seed of 0.
    // However, as updates accumulate, the EMA’s memory of this initial seed
    // fades exponentially, mitigating the initial skew.
    //
    // This is to eliminate seeding the initial EMA value from the
    // Update() method.
    private long _ema;

    public ExponentialMovingAverage() =>
        _alpha = 0.2D;

    public ExponentialMovingAverage(double alpha)
    {
        _alpha = Math.Clamp(alpha, 0.0, 1.0);
    }

    public void Update(long sample)
    {
        long ema, updatedEma;

        do
        {
            ema = Volatile.Read(ref _ema);
            updatedEma =
                (long)(_alpha * sample + (1 - _alpha) * ema);
        } while (Interlocked.CompareExchange(
                     ref _ema, updatedEma, ema) != ema);
    }

    public TimeSpan ReadTimeSpan()
    {
        var ema = Volatile.Read(ref _ema);
        return TimeSpan.FromTicks(ema);
    }

    public long ReadLong()
    {
        return Volatile.Read(ref _ema);
    }
}