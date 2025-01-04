using JotDB.Memory;

namespace JotDB.Pages;

public sealed class CircularList<T>
{
    private T[] _array;
    private long _head;
    private Lock _lock = new();

    public CircularList(long size)
    {
        if ((size & (size - 1)) != 0)
            throw new ArgumentException("size must be a power of 2.",
                nameof(size));

        _array = new T[size];
        Size = size;
    }

    public long Size { get; }

    public void Add(T item)
    {
        lock (_lock)
        {
            _array[_head] = item;
            Console.WriteLine($"inserting into position {_head}");
            _head = (_head + 1) & (Size - 1);
        }
    }
}

public sealed class PageBuffer
{
    private CircularList<ulong> _cache = new(4096);

    public void Write(DatabaseTransaction transaction)
    {
        _cache.Add(transaction.TransactionSequenceNumber);
    }
}