namespace JotDB;

public sealed class AppendOnlyFile : IAsyncDisposable
{
    private readonly FileStream _file;
    private long _offset;

    public AppendOnlyFile(string path)
    {
        _file = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read);
    }

    public void Write(ReadOnlySpan<byte> buffer)
    {
        RandomAccess.Write(_file.SafeFileHandle, buffer, _offset);
        _offset += buffer.Length;
    }

    public void Flush() => RandomAccess.FlushToDisk(_file.SafeFileHandle);

    public ValueTask DisposeAsync() => _file.DisposeAsync();
}
