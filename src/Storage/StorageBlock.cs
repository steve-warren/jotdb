using System.Runtime.CompilerServices;

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

    public unsafe void Write(ref WriteAheadLogTransactionHeader header)
    {
        Unsafe.Write(Memory.Pointer, header);
        BytesWritten += WriteAheadLogTransactionHeader.Size;
    }

    public void Write(
        ReadOnlySpan<byte> buffer)
    {
        var span = Memory.Span;

        buffer.CopyTo(span[BytesWritten..]);
        BytesWritten += buffer.Length;
    }

    public void ZeroUnusedBytes() =>
        Memory.Span[BytesWritten..].Clear();

    public void Reset()
    {
        BytesWritten = 0;
    }
}