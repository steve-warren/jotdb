using System.Diagnostics;
using System.Runtime.CompilerServices;
using JotDB.Storage.Journal;

namespace JotDB.Storage;

public sealed class StorageBlock
{
    private const nuint SIZE = 4096;

    public StorageBlock(
        ulong pageNumber,
        ulong timestamp,
        AlignedMemory memory)
    {
        PageNumber = pageNumber;
        Timestamp = timestamp;
        Memory = memory;
    }

    public ulong PageNumber { get; }
    public ulong Timestamp { get; }
    public int BytesWritten { get; private set; }
    public bool IsFull => (uint)BytesWritten == SIZE;
    public uint BytesAvailable => (uint)SIZE - (uint)BytesWritten;
    public int Size => (int)SIZE;
    public AlignedMemory Memory { get; }

    public unsafe bool TryWrite(void* data, uint size)
    {
        Unsafe.CopyBlock(destination: Memory.Pointer, source: data, byteCount: size);

        BytesWritten += (int) size;

        return true;
    }
    
    public bool TryWrite(
        ReadOnlySpan<byte> buffer)
    {
        if (BytesAvailable < buffer.Length)
            return false;

        var span = Memory.Span;

        buffer.CopyTo(span[BytesWritten..]);
        BytesWritten += buffer.Length;

        return true;
    }

    public void ZeroUnusedBytes() =>
        Memory.Span[BytesWritten..].Clear();

    public void Reset()
    {
        BytesWritten = 0;
    }
}