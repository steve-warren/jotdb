using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;

namespace JotDB.Storage.Journal;

[StructLayout(LayoutKind.Explicit, Pack = 1)]
public unsafe struct WriteAheadLogTransactionHeader
{
    public static readonly int Size = Unsafe
        .SizeOf<WriteAheadLogTransactionHeader>();

    [FieldOffset(0)]
    public uint StorageTransactionSequenceNumber;
    [FieldOffset(4)]
    public uint CommitSequenceNumber;
    [FieldOffset(8)]
    public ulong DatabaseTransactionSequenceNumber;
    [FieldOffset(16)]
    public int TransactionType;
    [FieldOffset(20)]
    public int DataLength;
    [FieldOffset(24)]
    public long Timestamp;
    [FieldOffset(32)]
    public fixed byte Hash[MD5.HashSizeInBytes];
}