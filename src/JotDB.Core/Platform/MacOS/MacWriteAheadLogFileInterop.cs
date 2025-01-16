using System.Diagnostics;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace JotDB.Platform.MacOS;

public static class MacWriteAheadLogFileInterop
{
    const int F_NOCACHE = 48;

    public static SafeFileHandle OpenFileHandle(
        string path,
        int preallocationSize)
    {
        var handle = File.OpenHandle(
            path: path,
            mode: FileMode.Create,
            access: FileAccess.ReadWrite,
            share: FileShare.None,
            options: FileOptions.WriteThrough,
            preallocationSize: preallocationSize);

        var fileDescriptor = handle.DangerousGetHandle().ToInt32();
        var result = MacSyscall.fcntl(fileDescriptor, F_NOCACHE, 1);

        Debug.Assert(result != -1, Marshal.GetLastPInvokeErrorMessage());

        return handle;
    }
}