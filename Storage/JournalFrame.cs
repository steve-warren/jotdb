using System.Runtime.InteropServices;

namespace JotDB.Storage;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct JournalFrame
{
    public ulong TransactionId;
    public long Timestamp;
    public JournalEntryOptions Flags;
    public ulong PageOffset;
}