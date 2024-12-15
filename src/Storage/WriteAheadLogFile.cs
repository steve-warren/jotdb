using System.Diagnostics;
using System.Runtime.InteropServices;
using JotDB.Platform.MacOS;
using Microsoft.Win32.SafeHandles;

namespace JotDB.Storage;

public sealed class WriteAheadLogFile : IDisposable
{
    private readonly SafeFileHandle _fileHandle;
    private long _offset;

    public static WriteAheadLogFile Open(
        string path)
    {
        return new WriteAheadLogFile(
            path: path,
            offset: 0);
    }

    private static SafeFileHandle OpenFileHandle(string path)
    {
        var fileOptions = FileOptions.WriteThrough;

        if (OperatingSystem.IsWindows())
        {
            const FileOptions FILE_FLAG_NO_BUFFERING = (FileOptions)0x20000000;
            fileOptions |= FILE_FLAG_NO_BUFFERING;
        }

        var handle = File.OpenHandle(
            path: path,
            mode: FileMode.OpenOrCreate,
            access: FileAccess.ReadWrite,
            share: FileShare.None,
            options: fileOptions);

        if (!OperatingSystem.IsMacOS()) return handle;

        const int F_NOCACHE = 48;

        var fileDescriptor = handle.DangerousGetHandle().ToInt32();
        var result = MacSyscall.fcntl(fileDescriptor, F_NOCACHE, 1);

        Debug.Assert(result != -1, Marshal.GetLastPInvokeErrorMessage());

        return handle;
    }

    private WriteAheadLogFile(
        string path,
        long offset)
    {
        Path = path;
        _offset = offset;
        _fileHandle = OpenFileHandle(path);
    }

    public string Path { get; }

    public void Dispose()
    {
        _fileHandle.Dispose();
    }

    public void FlushToDisk() => RandomAccess.FlushToDisk(_fileHandle);

    public void WriteToDisk(
        LinkedList<StorageBlock> blocks)
    {
        Console.WriteLine($"writing {blocks.Count} blocks to journal on disk.");

        foreach (var block in blocks)
        {
            block.ZeroUnusedBytes();
            RandomAccess.Write(_fileHandle, block.Memory.Span, _offset);
            _offset += block.Size;
        }
    }
}