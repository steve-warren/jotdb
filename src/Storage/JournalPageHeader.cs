using System.Runtime.InteropServices;

namespace JotDB.Storage;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct JournalPageHeader
{
    public ulong TransactionId;
    public long Timestamp;
    public JournalEntryOptions Flags;
    public ulong PageOffset;
    [MarshalAs(UnmanagedType.ByValArray, SizeConst = 16)]
    public byte[] Checksum;
}