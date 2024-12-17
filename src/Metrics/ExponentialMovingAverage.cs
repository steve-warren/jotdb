namespace JotDB.Metrics;

public struct ExponentialMovingAverage
{
    private readonly double _alpha = 0.2;
    private double? _ema;
    private readonly Lock _lock = new();

    public ExponentialMovingAverage(double alpha)
    {
        _alpha = Math.Clamp(alpha, 0.0, 1.0);
    }

    public void Update(double sample)
    {
        lock (_lock)
        {
            _ema = _ema is null
                ? sample
                : _alpha * sample + (1 - _alpha) * _ema;
        }
    }

    public readonly double Value
    {
        get
        {
            lock (_lock)
            {
                return _ema.GetValueOrDefault();
            }
        }
    }
}