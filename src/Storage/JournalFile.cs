using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using JotDB.Platform.MacOS;
using Microsoft.Win32.SafeHandles;

namespace JotDB.Storage;

public sealed class JournalFile : IDisposable
{
    private readonly SafeFileHandle _file;
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

        Debug.Assert(result != -1, "fcntl returned an error");

        return handle;
    }

    private JournalFile(
        string path,
        long offset)
    {
        Path = path;
        _offset = offset;
        _file = OpenFileHandle(path);
    }

    public string Path { get; }

    public void Dispose()
    {
        _file.Dispose();
    }

    public void FlushToDisk() => RandomAccess.FlushToDisk(_file);

    public void WriteToDisk(
        List<DataPage> pages)
    {
        Console.WriteLine("writing to journal on disk.");

        foreach (var page in pages)
        {
            RandomAccess.Write(_file, page.Span, _offset);
            _offset += page.Size;
        }
    }
}