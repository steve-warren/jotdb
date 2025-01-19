using System.Runtime.InteropServices;

namespace JotDB.Storage.Documents;

[StructLayout(LayoutKind.Explicit, Pack = 0)]
public unsafe struct DocumentCollectionHeader
{
    [FieldOffset(0)] public uint MagicNumber;
    [FieldOffset(4)] public uint Version;
    [FieldOffset(8)] public Guid Id;
}