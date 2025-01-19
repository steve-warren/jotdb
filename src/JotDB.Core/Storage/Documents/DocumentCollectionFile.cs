using System.Diagnostics;
using System.Runtime.CompilerServices;
using JotDB.Memory;
using Microsoft.Win32.SafeHandles;

namespace JotDB.Storage.Documents;

public class DocumentCollectionFile : IDisposable
{
    private readonly SafeFileHandle _fileHandle;

    private DocumentCollectionFile(
        SafeFileHandle handle,
        long size)
    {
        _fileHandle = handle;
        Size = size;
    }

    ~DocumentCollectionFile()
    {
        Dispose();
    }

    public long Size { get; }

    public void Dispose()
    {
        _fileHandle.Dispose();
        GC.SuppressFinalize(this);
    }

    public static DocumentCollectionFile Open(
        string collectionName)
    {
        var handle = File.OpenHandle(
            path: $"{collectionName}.db",
            mode: FileMode.Open,
            access: FileAccess.ReadWrite,
            share: FileShare.None,
            options: FileOptions.RandomAccess);

        var info = new FileInfo($"{collectionName}.db");

        return new DocumentCollectionFile(handle, info.Length);
    }

    public static DocumentCollectionFile Create(
        string collectionName,
        long preallocationSize)
    {
        var info = new FileInfo(collectionName);

        Debug.Assert(info.Exists is false, "file should not exist");

        var handle = File.OpenHandle(
            path: $"{collectionName}.db",
            mode: FileMode.Create,
            access: FileAccess.ReadWrite,
            share: FileShare.None,
            options: FileOptions.RandomAccess,
            preallocationSize: preallocationSize);

        return new DocumentCollectionFile(
            handle: handle,
            size: Unsafe.SizeOf<DocumentCollectionHeader>());
    }

    public static bool Exists(string collectionName)
        => File.Exists($"{collectionName}.db");

    public void Write(ReadOnlySpan<byte> data, long offset)
        => RandomAccess.Write(_fileHandle, data, offset);

    public void Read(Span<byte> data, long offset)
        => RandomAccess.Read(_fileHandle, data, offset);
}