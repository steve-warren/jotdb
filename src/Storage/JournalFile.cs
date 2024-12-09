using System.Diagnostics;
using System.Runtime.InteropServices;
using JotDB.Platform.MacOS;
using Microsoft.Win32.SafeHandles;

namespace JotDB.Storage;

public sealed class JournalFile : IDisposable
{
    private readonly SafeFileHandle _fileHandle;
    private long _offset;

    public static JournalFile Open(
        string path)
    {
        return new JournalFile(
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

    private JournalFile(
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
        HashSet<JournalPage> pages)
    {
        Console.WriteLine("writing to journal on disk.");

        foreach (var page in pages)
        {
            page.ZeroUnusedBytes();
            RandomAccess.Write(_fileHandle, page.Span, _offset);
            _offset += page.Size;
        }
    }
}