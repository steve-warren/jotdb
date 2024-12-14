using System.Runtime.InteropServices;

namespace JotDB.Storage;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct TransactionHeader
{
    public ulong TransactionSequenceNumber;
    public int DataLength;
    public ulong PageNumber;
    public long Timestamp;
    [MarshalAs(UnmanagedType.ByValArray, SizeConst = 16)]
    public byte[] Hash;
}