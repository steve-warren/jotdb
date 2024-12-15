namespace JotDB.Memory;

public sealed class AlignedMemoryPool : IDisposable
{
    private readonly Stack<AlignedMemory> _pool = [];
    private bool _disposed;

    public static readonly AlignedMemoryPool Default = new(1024);

    public AlignedMemoryPool(int capacity)
    {
        Capacity = capacity;
    }

    ~AlignedMemoryPool() =>
        Dispose();

    public int Capacity { get; }
    public HashSet<AlignedMemory> Rented { get; } = [];

    public AlignedMemory Rent()
    {
        var memory = _pool.Count == 0
            ? AlignedMemory.Allocate(4096, 4096)
            : _pool.Pop();

        Rented.Add(memory);

        return memory;
    }

    public void Return(AlignedMemory memory)
    {
        if (Rented.Remove(memory))
            _pool.Push(memory);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        foreach (var memory in _pool)
            memory.Dispose();

        _pool.Clear();

        GC.SuppressFinalize(this);
    }
}