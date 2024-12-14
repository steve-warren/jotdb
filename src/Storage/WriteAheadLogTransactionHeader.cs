using System.Runtime.InteropServices;
using System.Security.Cryptography;

namespace JotDB.Storage;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct WriteAheadLogTransactionHeader
{
    public ulong TransactionSequenceNumber;
    public ulong CommitSequenceNumber;
    public byte TransactionType;
    public int DataLength;
    public ulong PageNumber;
    public long Timestamp;
    [MarshalAs(UnmanagedType.ByValArray, SizeConst = MD5.HashSizeInBytes)]
    public byte[] Hash;
}