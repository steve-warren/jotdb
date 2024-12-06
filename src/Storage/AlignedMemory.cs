using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace JotDB.Storage;

public readonly unsafe ref struct AlignedMemory
{
    private readonly Span<byte> _span;

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

        Pointer = pointer;
        Size = (int)size;
        Alignment = (int)alignment;
    }

    public int Size { get; }
    public int Alignment { get; }
    public Span<byte> Span =>
        _span;
    public void* Pointer { get; }

    public void Dispose() =>
        NativeMemory.AlignedFree(Pointer);

    public void Clear() =>
        _span.Clear();
}