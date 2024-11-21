using System.Diagnostics;

namespace JotDB;

public class DataPage(uint id)
{
    public const uint MAX_PAGE_SIZE = 4096; // 4 KiB

    private List<ReadOnlyMemory<byte>> _data = [];

    public uint Id { get; } = id;
    public int Size { get; private set; }

    public bool TryWrite(ReadOnlyMemory<byte> data)
    {
        Debug.Assert(data.Length <= MAX_PAGE_SIZE, "data length exceeds page size.");

        if (Size + data.Length > MAX_PAGE_SIZE)
            return false;
        
        _data.Add(data);

        Size += data.Length;
        return true;
    }
}