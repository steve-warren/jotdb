using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace JotDB.Storage;

public readonly unsafe struct AlignedMemory
{
    private readonly void* _pointer;

    /// <summary>
    /// Allocates a new <see cref="AlignedMemory"/> with the specified size and alignment.
    /// </summary>
    /// <param name="size">The size of the memory block to allocate, in bytes.</param>
    /// <param name="alignment">The alignment requirement for the memory block, in bytes.</param>
    /// <returns>A new instance of <see cref="AlignedMemory"/> with the specified size and alignment.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static AlignedMemory Allocate(
        nuint size,
        nuint alignment)
    {
        return new AlignedMemory(
            size,
            alignment);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private AlignedMemory(
        nuint size,
        nuint alignment)
    {
        _pointer = NativeMemory.AlignedAlloc(size, alignment);
        Size = (int)size;
    }

    public int Size { get; }

    public Span<byte> Span =>
        new(_pointer, Size);

    public void Dispose()
    {
        NativeMemory.AlignedFree(_pointer);
    }
}