using System.Runtime.CompilerServices;

namespace JotDB.Memory;

public struct AlignedMemoryWriter(AlignedMemory memory, int bytesWritten = 0)
{
    public AlignedMemory Memory { get; } = memory;
    public int BytesWritten { get; private set; } = bytesWritten;
    public readonly bool IsFull => (uint)BytesWritten == Memory.Size;
    public readonly uint BytesAvailable => (uint)Memory.Size - (uint)BytesWritten;
    
    public unsafe void Write<T>(T value)
        where T : allows ref struct
    {
        Unsafe.Write(Memory.Pointer, value);
        BytesWritten += Unsafe.SizeOf<T>();
    }

    public void Write(ReadOnlySpan<byte> buffer)
    {
        var span = Memory.Span;
        buffer.CopyTo(span[BytesWritten..]);
        BytesWritten += buffer.Length;
    }

    public readonly void ZeroRemainingBytes() =>
        Memory.Span[BytesWritten..].Clear();

    public void Reset() =>
        BytesWritten = 0;
}