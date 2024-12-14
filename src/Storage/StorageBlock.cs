using System.Diagnostics;

namespace JotDB.Storage;

public sealed class StorageBlock : IDisposable
{
    private bool _disposed;
    private const nuint SIZE = 4096;
    private const nuint ALIGNMENT = 4096;
    private readonly AlignedMemory _memory;

    public StorageBlock(
        ulong pageNumber,
        ulong timestamp,
        AlignedMemory memory)
    {
        PageNumber = pageNumber;
        Timestamp = timestamp;
        _memory = memory;
    }

    public ulong PageNumber { get; }
    public ulong Timestamp { get; }
    public int BytesWritten { get; private set; }
    public bool IsFull => (uint)BytesWritten == SIZE;
    public uint BytesAvailable => (uint)SIZE - (uint)BytesWritten;
    public int Size => (int)SIZE;
    public AlignedMemory Memory => _memory;
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

    public void ZeroUnusedBytes() =>
        _memory.Span[BytesWritten..].Clear();

    public void Reset()
    {
        BytesWritten = 0;
    }

    public void Dispose()
    {
        Debug.Assert(_disposed == false, "already disposed.");
        if (_disposed)
            return;
        _disposed = true;
        _memory.Dispose();
    }
}