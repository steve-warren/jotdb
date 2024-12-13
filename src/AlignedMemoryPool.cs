using JotDB.Storage;

namespace JotDB;

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

    public Releaser Rent(out AlignedMemory memory)
    {
        memory = _pool.Count == 0
            ? AlignedMemory.Allocate(4096, 4096)
            : _pool.Pop();

        Rented.Add(memory);

        return new Releaser(this, memory);
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

    public readonly ref struct Releaser
    {
        private readonly AlignedMemoryPool _pool;
        private readonly AlignedMemory _memory;

        public Releaser(
            AlignedMemoryPool pool,
            AlignedMemory memory)
        {
            _pool = pool;
            _memory = memory;
        }

        public void Dispose()
        {
            _pool.Return(_memory);
        }
    }
}