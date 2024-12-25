using System.Diagnostics;
using System.Runtime.InteropServices;
using JotDB.Platform.MacOS;
using Microsoft.Win32.SafeHandles;

namespace JotDB.Storage.Journal;

public sealed class SafeFileHandleWriteAheadLogFile : WriteAheadLogFile
{
    private const int MAX_FILE_SIZE = 4 * 1024 * 1024;

    private SafeFileHandle _fileHandle;

    public static SafeFileHandleWriteAheadLogFile Open()
    {
        return new SafeFileHandleWriteAheadLogFile(
            offset: 0);
    }

    private static SafeFileHandle OpenFileHandle()
    {
        var fileOptions = FileOptions.WriteThrough;

        if (OperatingSystem.IsWindows())
        {
            const FileOptions FILE_FLAG_NO_BUFFERING = (FileOptions)0x20000000;
            fileOptions |= FILE_FLAG_NO_BUFFERING;
        }

        var path = $"journal_{DateTime
            .Now:yyyyMMddHHmmss}.txt";

        var handle = File.OpenHandle(
            path: path,
            mode: FileMode.Create,
            access: FileAccess.ReadWrite,
            share: FileShare.None,
            options: fileOptions,
            preallocationSize: 4 * 1024 * 1024);

        if (!OperatingSystem.IsMacOS()) return handle;

        const int F_NOCACHE = 48;

        var fileDescriptor = handle.DangerousGetHandle().ToInt32();
        var result = MacSyscall.fcntl(fileDescriptor, F_NOCACHE, 1);

        Debug.Assert(result != -1, Marshal.GetLastPInvokeErrorMessage());

        return handle;
    }

    private SafeFileHandleWriteAheadLogFile(
        long offset)
    {
        Offset = offset;
        _fileHandle = OpenFileHandle();
    }

    public long Offset { get; private set; }

    public override bool Rotate()
    {
        if (Offset < MAX_FILE_SIZE) return false;

        var newHandle = OpenFileHandle();

        Offset = 0;
        var oldHandle = _fileHandle;
        _fileHandle = newHandle;
        RandomAccess.FlushToDisk(oldHandle);
        oldHandle.Dispose();

        return true;
    }

    public override void Dispose()
    {
        _fileHandle.Dispose();
    }

    public override void Flush() => RandomAccess.FlushToDisk(_fileHandle);

    public override void Write(ReadOnlySpan<byte> span)
    {
        RandomAccess.Write(_fileHandle, span, Offset);
        Offset += span.Length;
    }
}