using System.Runtime.CompilerServices;
using System.Security.Cryptography;
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
        const FileOptions FILE_FLAG_NO_BUFFERING = (FileOptions)0x20000000;
        var fileOptions = FileOptions.WriteThrough;

        if (OperatingSystem.IsWindows())
            fileOptions |= FILE_FLAG_NO_BUFFERING;

        return File.OpenHandle(
            path: path,
            mode: FileMode.OpenOrCreate,
            access: FileAccess.ReadWrite,
            share: FileShare.None,
            options: fileOptions);
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

    public unsafe void WriteToDisk(
        LinkedList<DataPage> transactions)
    {
        Console.WriteLine("writing to journal on disk.");
    }
}