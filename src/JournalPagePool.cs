using JotDB.Storage;

namespace JotDB;

public sealed unsafe class JournalPagePool : IDisposable
{
    private readonly delegate*<JournalPage> _factory;
    private readonly Stack<JournalPage> _pool = [];

    public JournalPagePool(int capacity, delegate*<JournalPage> factory)
    {
        Capacity = capacity;
        _factory = factory;
    }

    public int Capacity { get; }
    public HashSet<JournalPage> Rented { get; } = [];

    public JournalPage Rent()
    {
        var page = _pool.Count == 0 ? _factory() : _pool.Pop();

        Rented.Add(page);

        return page;
    }

    public void Return(JournalPage page)
    {
        if(Rented.Remove(page))
            _pool.Push(page);
    }

    public void Dispose()
    {
        foreach (var page in _pool)
            page.Dispose();
    }
}