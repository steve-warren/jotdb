using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using Microsoft.Win32.SafeHandles;

namespace JotDB.Storage.Documents;

public sealed class Page : IDisposable
{
    private readonly MemoryMappedViewAccessor _mmap;

    public Page(
        int number,
        MemoryMappedViewAccessor mmap)
    {
        Number = number;
        _mmap = mmap;
    }

    ~Page()
    {
        Dispose();
    }

    public void Dispose()
    {
        _mmap.Dispose();

        GC.SuppressFinalize(this);
    }

    public int Number { get; }

    public void Write(ReadOnlySpan<byte> buffer)
    {
        _mmap.SafeMemoryMappedViewHandle.WriteSpan(0, buffer);
    }
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
            Capacity.Mebibytes(4));
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