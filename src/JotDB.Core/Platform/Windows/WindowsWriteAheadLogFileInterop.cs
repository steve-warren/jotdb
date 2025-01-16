using Microsoft.Win32.SafeHandles;

namespace JotDB.Platform.Windows;

public static class WindowsWriteAheadLogFileInterop
{
    const FileOptions FILE_FLAG_NO_BUFFERING = (FileOptions)0x20000000;

    public static SafeFileHandle OpenFileHandle(string path, int preallocationSize)
    {
        return File.OpenHandle(
            path: path,
            mode: FileMode.Create,
            access: FileAccess.ReadWrite,
            share: FileShare.None,
            options: FileOptions.WriteThrough | FILE_FLAG_NO_BUFFERING,
            preallocationSize: preallocationSize);
    }
}