using System.Runtime.CompilerServices;

namespace JotDB.Storage.Documents;

public static class Capacity
{
    private const long BytesPerKibibyte = 1024;
    private const long BytesPerMebibyte = 1024 * 1024;
    private const long BytesPerGibibyte = 1024 * 1024 * 1024;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long Mebibytes(long mebibytes) =>
        mebibytes * BytesPerMebibyte;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long Kibibytes(long kibibytes) =>
        kibibytes * BytesPerKibibyte;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long Gibibytes(long gibibytes) =>
        gibibytes * BytesPerGibibyte;

    public static class Int32
    {
        private const int Int32BytesPerKibibyte = 1024;
        private const int Int32BytesPerMebibyte = 1024 * 1024;
        private const int Int32BytesPerGibibyte = 1024 * 1024 * 1024;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Mebibytes(int mebibytes) =>
            mebibytes * Int32BytesPerMebibyte;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Kibibytes(int kibibytes) =>
            kibibytes * Int32BytesPerKibibyte;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Gibibytes(int gibibytes) =>
            gibibytes * Int32BytesPerGibibyte;
    }

    public static class UIntPtr
    {
        private const nuint UIntPtrBytesPerKibibyte = 1024;
        private const nuint UIntPtrBytesPerMebibyte = 1024 * 1024;
        private const nuint UIntPtrBytesPerGibibyte = 1024 * 1024 * 1024;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint Mebibytes(nuint mebibytes) =>
            mebibytes * UIntPtrBytesPerMebibyte;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint Kibibytes(nuint kibibytes) =>
            kibibytes * UIntPtrBytesPerKibibyte;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint Gibibytes(nuint gibibytes) =>
            gibibytes * UIntPtrBytesPerGibibyte;
    }
}