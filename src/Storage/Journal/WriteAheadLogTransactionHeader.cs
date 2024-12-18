using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace JotDB.Storage.Journal;

[StructLayout(LayoutKind.Explicit, Pack = 1)]
public ref struct WriteAheadLogTransactionHeader
{
    public static readonly int Size = Unsafe
        .SizeOf<WriteAheadLogTransactionHeader>();

    [FieldOffset(0)]
    public ulong TransactionSequenceNumber;
    [FieldOffset(8)]
    public ulong CommitSequenceNumber;
    [FieldOffset(16)]
    public int TransactionType;
    [FieldOffset(20)]
    public int DataLength;
    [FieldOffset(24)]
    public long Timestamp;
    [FieldOffset(32)]
    [MarshalAs(UnmanagedType.ByValArray, SizeConst = 16)]
    public byte[] Hash;
}