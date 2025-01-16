using System.Runtime.InteropServices;

namespace JotDB.Storage.Documents;

[StructLayout(LayoutKind.Explicit, Pack = 1)]
public unsafe struct DocumentCollectionHeader
{
    // 8011ACFB
    [FieldOffset(0)] public uint MagicNumber;
    [FieldOffset(4)] public uint Version;
}