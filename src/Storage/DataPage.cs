namespace JotDB.Storage;

public class DataPage
{
    private const nuint SIZE = 4096;
    private const nuint ALIGNMENT = 4096;
    private readonly AlignedMemory _memory;

    public DataPage()
    {
        _memory = AlignedMemory.Allocate(SIZE, ALIGNMENT);
    }

    public void Write(
        ReadOnlySpan<byte> buffer,
        int offset)
    {
        var span = _memory.Span;

        buffer.CopyTo(span[offset..]);
    }
}