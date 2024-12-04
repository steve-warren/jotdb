using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace JotDB.Storage;

public readonly unsafe ref struct AlignedMemory
{
    private readonly void* _pointer;
    private readonly Span<byte> _span;

    /// <summary>
    /// Allocates a new <see cref="AlignedMemory"/> with the specified size and alignment.
    /// </summary>
    /// <param name="pageSize">The size of the memory block to allocate, in bytes.</param>
    /// <param name="alignment">The alignment requirement for the memory block, in bytes.</param>
    /// <returns>A new instance of <see cref="AlignedMemory"/> with the specified size and alignment.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static AlignedMemory Allocate(
        nuint pageSize,
        nuint alignment)
    {
        return new AlignedMemory(
            pageSize,
            alignment);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private AlignedMemory(
        nuint size,
        nuint alignment)
    {
        var pointer = NativeMemory.AlignedAlloc(size, alignment);

        try
        {
            _span = new Span<byte>(pointer, (int)size);
            _span.Clear();
        }

        catch
        {
            Dispose();
            throw;
        }

        _pointer = pointer;
        Size = (int)size;
        Alignment = (int)alignment;
    }

    public int Size { get; }
    public int Alignment { get; }
    public Span<byte> Span =>
        _span;

    public void Dispose() =>
        NativeMemory.AlignedFree(_pointer);

    public void Clear() =>
        _span.Clear();
}