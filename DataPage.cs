using System.Diagnostics;

namespace JotDB;

public class DataPage(uint id)
{
    public const uint PAGE_SIZE = 4096; // 4 KiB

    private ReadOnlyMemory<byte> _data;

    public uint Id { get; } = id;

    public void Write(ReadOnlyMemory<byte> data)
    {
        Debug.Assert(data.Length <= PAGE_SIZE, "data length exceeds page size.");

        _data = data;
    }

    public ReadOnlyMemory<byte> Read() => _data;
}