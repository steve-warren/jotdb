using System.Runtime.InteropServices;

namespace JotDB.Platform.MacOS;

public static partial class MacSyscall
{
    private const string LIBC = "libc";

    [LibraryImport(LIBC, SetLastError = true)]
    public static partial int syscall(long number, long arg1, long arg2, long arg3);

    [LibraryImport(LIBC, SetLastError = true)]
    public static partial int fcntl(int fd, int cmd, int arg);
}