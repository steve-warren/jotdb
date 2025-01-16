using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace JotDB.Memory;

public sealed unsafe class AlignedMemory : IDisposable,
    IEquatable<AlignedMemory>
{
    private static uint sequenceNumber_;
    private readonly void* _pointer;
    private bool _disposed;
    private readonly uint _id;

    /// <summary>
    /// Allocates a new <see cref="AlignedMemory"/> with the specified size and alignment.
    /// </summary>
    /// <param name="size">The size of the memory block to allocate, in bytes.</param>
    /// <param name="alignment">The alignment requirement for the memory block, in bytes.</param>
    /// <returns>A new instance of <see cref="AlignedMemory"/> with the specified size and alignment.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static AlignedMemory Allocate(
        nuint size = 4096,
        nuint alignment = 4096)
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
        _id = Interlocked.Increment(ref sequenceNumber_);
        _pointer = NativeMemory.AlignedAlloc(size, alignment);

        NativeMemory.Clear((byte*)_pointer, size);

        Size = (int)size;
        Alignment = (int)alignment;
    }

    ~AlignedMemory()
    {
        Dispose();
    }

    public uint Id => _id;
    public int Size { get; }
    public int Alignment { get; }
    public void* Pointer => _pointer;

    public Span<byte> Span =>
        new(_pointer, Size);

    public void Dispose()
    {
        if (_disposed) return;

        _disposed = true;
        NativeMemory.AlignedFree(_pointer);

        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Provides a slice of the memory block as a read-only span, starting at the specified offset and extending for the specified length.
    /// </summary>
    /// <param name="start">The zero-based start index within the memory block.</param>
    /// <param name="length">The number of bytes in the slice.</param>
    /// <returns>A <see cref="ReadOnlySpan{T}"/> representing the specified portion of the memory block.</returns>
    public ReadOnlySpan<byte> DangerousSlice(
        int start,
        int length)
    {
        return new ReadOnlySpan<byte>(
            (byte*)_pointer + start, length);
    }

    /// <summary>
    /// Clears a specified portion of the memory block by setting its contents to zero, starting at the given offset and continuing for the specified length.
    /// </summary>
    /// <param name="start">The zero-based start index within the memory block to begin clearing.</param>
    /// <param name="length">The number of bytes to clear.</param>
    public void DangerousClear(
        int start,
        int length)
    {
        NativeMemory.Clear((byte*)_pointer + start, (nuint)length);
    }

    public bool Equals(AlignedMemory? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return _pointer == other._pointer;
    }

    public override bool Equals(object? obj)
    {
        return ReferenceEquals(this, obj) ||
               obj is AlignedMemory other && Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(unchecked((int)(long)_pointer));
    }
}