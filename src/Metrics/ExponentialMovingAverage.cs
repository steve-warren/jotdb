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

    public ExponentialMovingAverage(double alpha)
    {
        _alpha = Math.Clamp(alpha, 0.0, 1.0);
    }

    public void Update(TimeSpan sample)
    {
        long ema, updatedEma;

        do
        {
            ema = Volatile.Read(ref _ema);
            updatedEma =
                (long)(_alpha * sample.Ticks + (1 - _alpha) * ema);
        } while (Interlocked.CompareExchange(
                     ref _ema, updatedEma, ema) != ema);
    }

    public TimeSpan Value
    {
        get
        {
            var ema = Volatile.Read(ref _ema);
            return TimeSpan.FromTicks(ema);
        }
    }
}