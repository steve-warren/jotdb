using JotDB.Storage;

namespace JotDB;

public sealed class AlignedMemoryPool : IDisposable
{
    private readonly Stack<AlignedMemory> _pool = [];

    public AlignedMemoryPool(int capacity)
    {
        Capacity = capacity;
    }

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
        if(Rented.Remove(memory))
            _pool.Push(memory);
    }

    public void Dispose()
    {
        foreach (var memory in _pool)
            memory.Dispose();
    }
}