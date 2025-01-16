using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace JotDB.Memory;

public ref struct AlignedMemoryWriter
{
    public AlignedMemoryWriter(
        AlignedMemory memory,
        int bytesWritten = 0)
    {
        Memory = memory;
        BytesWritten = bytesWritten;
    }

    public AlignedMemory Memory { get; }
    public int BytesWritten { get; private set; }
    public readonly bool IsFull => (uint)BytesWritten == Memory.Size;

    public readonly uint BytesAvailable =>
        (uint)Memory.Size - (uint)BytesWritten;

    /// <summary>
    /// Gets a read-only span of bytes representing the memory region of the writer
    /// that is aligned to the nearest 4 KiB boundary based on the number of bytes written.
    /// </summary>
    /// <remarks>
    /// The aligned span size is rounded up to the nearest 4 KiB increment to ensure proper alignment.
    /// The resulting span's size is never greater than the total memory size of the underlying aligned memory.
    /// </remarks>
    /// <value>
    /// A read-only span of bytes that represents the aligned memory slice.
    /// </value>
    /// <exception cref="Debug.Assert">Occurs if the calculated aligned size exceeds the total memory size.</exception>
    public readonly ReadOnlySpan<byte> AlignedSpan
    {
        get
        {
            // round to nearest 4KiB number
            var alignedSize = (BytesWritten + 4095) & ~4095;

            Debug.Assert(alignedSize <= Memory.Size);

            var slice = Memory.DangerousSlice(0, alignedSize);

            return slice;
        }
    }

    /// <summary>
    /// Clears unused bytes in the memory buffer, aligning the size to the nearest 4KiB boundary.
    /// </summary>
    /// <remarks>
    /// This method ensures that any remaining unused memory in the buffer is reset to zero while maintaining proper alignment.
    /// It calculates the aligned size by rounding the number of written bytes up to the next multiple of 4KiB.
    /// </remarks>
    /// <exception cref="System.Diagnostics.Debug.AssertException">
    /// Thrown if the calculated aligned size exceeds the size of the memory buffer.
    /// </exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void ZeroPaddingBytesAligned()
    {
        // round to nearest 4KiB number
        var alignedSize = (BytesWritten + 4095) & ~4095;

        Debug.Assert(alignedSize <= Memory.Size);

        Memory.DangerousClear(
            start: BytesWritten,
            length: alignedSize - BytesWritten);
    }

    public readonly void ZeroUsedBytes()
    {
        Memory.DangerousClear(
            0,
            BytesWritten);
    }

    public unsafe void Write<T>(T value)
        where T : allows ref struct
    {
        Unsafe.Write(Memory.Pointer, value);
        BytesWritten += Unsafe.SizeOf<T>();
    }

    public unsafe void Write<T>(T* value) where T : unmanaged
    {
        Unsafe.Write(Memory.Pointer, *value);
        BytesWritten += Unsafe.SizeOf<T>();
    }

    public void Write(ReadOnlySpan<byte> source)
    {
        var span = Memory.Span;
        source.CopyTo(span[BytesWritten..]);
        BytesWritten += source.Length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly double GetFillPercentage() =>
        BytesWritten / (double)Memory.Size * 100;

    public readonly double GetFragmentationPercentage() =>
        (1 - BytesWritten / (double)Memory.Size) * 100;

    public void Reset() =>
        BytesWritten = 0;
}