using Microsoft.Win32.SafeHandles;

namespace JotDB.Storage.Documents;

public sealed class DocumentCollection : IDisposable
{
    private readonly SafeFileHandle _file;

    public static DocumentCollection Open(
        string path)
    {
        var file = File.OpenHandle(
            path,
            FileMode.OpenOrCreate,
            FileAccess.Read,
            FileShare.ReadWrite,
            FileOptions.RandomAccess | FileOptions.Asynchronous,
            preallocationSize: 4096);

        return new DocumentCollection(
            file);
    }

    private DocumentCollection(SafeFileHandle file)
    {
        _file = file;
    }

    public void Dispose()
    {
        // TODO release managed resources here
    }
}