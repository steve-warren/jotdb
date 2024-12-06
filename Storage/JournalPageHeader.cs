using System.Runtime.InteropServices;

namespace JotDB.Storage;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public unsafe struct JournalPageHeader
{
    public ulong TransactionId;
    public long Timestamp;
    public JournalEntryOptions Flags;
    public ulong PageOffset;
    public fixed byte Checksum[16];
}