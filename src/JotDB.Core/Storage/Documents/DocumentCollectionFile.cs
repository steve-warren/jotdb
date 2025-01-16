using System.Runtime.CompilerServices;
using JotDB.Memory;
using Microsoft.Win32.SafeHandles;

namespace JotDB.Storage.Documents;

public class DocumentCollectionFile : IDisposable
{
    private readonly SafeFileHandle _fileHandle = OpenFileHandle();
    private readonly AlignedMemory _fileBuffer;

    private DocumentCollectionFile()
    {
        _fileBuffer = AlignedMemory.Allocate();
        Initialize();
    }

    ~DocumentCollectionFile()
    {
        Dispose();
    }

    public static DocumentCollectionFile Open()
    {
        return new DocumentCollectionFile();
    }

    private static SafeFileHandle OpenFileHandle()
    {
        var path = $"jotdb_collection.db";

        if (OperatingSystem.IsMacOS())
        {
            var handle = File.OpenHandle(
                path: path,
                mode: FileMode.Create,
                access: FileAccess.ReadWrite,
                share: FileShare.None,
                options: FileOptions.RandomAccess,
                preallocationSize: Capacity.Mebibytes(1));

            return handle;
        }

        else
            throw new PlatformNotSupportedException();
    }

    public void Dispose()
    {
        _fileHandle.Dispose();
        _fileBuffer.Dispose();
        GC.SuppressFinalize(this);
    }

    private unsafe void Initialize()
    {
        DocumentCollectionHeader header;
        var p = &header;

        p->MagicNumber = 0x8011ACFB;
        p->Version = 1;

        Unsafe.Write(_fileBuffer.Pointer, *p);

        RandomAccess.Write(_fileHandle, _fileBuffer.Span, 0);
    }
}