using System.IO.MemoryMappedFiles;

namespace JotDB.Storage.Documents;

public sealed class Page : IDisposable
{
    private ulong _offset;
    private readonly MemoryMappedViewAccessor _memoryMap;

    public Page(
        int number,
        MemoryMappedViewAccessor memoryMap)
    {
        Number = number;
        _memoryMap = memoryMap;
    }

    ~Page()
    {
        Dispose();
    }

    public void Dispose()
    {
        _memoryMap.Dispose();

        GC.SuppressFinalize(this);
    }

    public int Number { get; }
    public long Size => _memoryMap.Capacity;
    public long BytesWritten => (long) _offset;
    public bool IsFull => BytesWritten >= _memoryMap.Capacity;

    public long BytesAvailable =>
        _memoryMap.Capacity - BytesWritten;

    public bool TryWrite(ReadOnlySpan<byte> buffer)
    {
        if (buffer.Length > BytesAvailable)
            return false;

        _memoryMap.SafeMemoryMappedViewHandle.WriteSpan(_offset, buffer);
        _offset += (ulong)buffer.Length;

        return true;
    }

    public double GetFillPercentage() =>
        BytesWritten / (double)_memoryMap.Capacity * 100;

    public double GetFragmentationPercentage() =>
        (1 - BytesWritten / (double)_memoryMap.Capacity) * 100;
}

public sealed class PageCollection : IDisposable
{
    private readonly MemoryMappedFile _file;
    private long _offset;
    private int _pageCount;

    public PageCollection()
    {
        _file = MemoryMappedFile.CreateNew(
            null,
            Capacity.Mebibytes(256));

        Root = Allocate();
    }

    ~PageCollection()
    {
        Dispose();
    }

    public void Dispose()
    {
        _file.Dispose();

        GC.SuppressFinalize(this);
    }
    
    public Page Root { get; private set; }

    public Page Allocate()
    {
        var view = _file.CreateViewAccessor(
            offset: _offset,
            size: Capacity.Kibibytes(4));

        Interlocked.Add(ref _offset, view.Capacity);
        Interlocked.Increment(ref _pageCount);

        return new Page(_pageCount, view);
    }
}