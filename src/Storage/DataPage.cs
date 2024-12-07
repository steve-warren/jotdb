using System.Diagnostics;

namespace JotDB.Storage;

public sealed class DataPage : IDisposable
{
    private const nuint SIZE = 4096;
    private const nuint ALIGNMENT = 4096;
    private readonly AlignedMemory _memory;

    public DataPage(
        ulong pageNumber,
        ulong timestamp)
    {
        PageNumber = pageNumber;
        Timestamp = timestamp;
        _memory = AlignedMemory.Allocate(SIZE, ALIGNMENT);
    }

    public ulong PageNumber { get; }
    public ulong Timestamp { get; }
    public int BytesWritten { get; private set; }
    public bool IsFull => (uint) BytesWritten == SIZE;
    public uint BytesAvailable => (uint) SIZE - (uint) BytesWritten;

    public bool TryWrite(
        ReadOnlySpan<byte> buffer)
    {
        if (BytesAvailable < buffer.Length)
            return false;

        var span = _memory.Span;

        buffer.CopyTo(span[BytesWritten..]);
        BytesWritten += buffer.Length;
        
        return true;
    }

    public void Dispose()
    {
        _memory.Dispose();
    }
}