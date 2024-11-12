namespace JotDB;

public sealed class AppendOnlyFile : IAsyncDisposable
{
    private readonly FileStream _file;
    private long _offset;
    private readonly ReadOnlyMemory<byte>[] _buffer;

    public AppendOnlyFile(string path)
    {
        _file = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read);
        _buffer = new ReadOnlyMemory<byte>[2];
    }

    public void Write(ReadOnlySpan<byte> buffer)
    {
        RandomAccess.Write(_file.SafeFileHandle, buffer, _offset);
        _offset += buffer.Length;
    }

    public void Write(ReadOnlyMemory<byte> buffer1, ReadOnlyMemory<byte> buffer2)
    {
        _buffer[0] = buffer1;
        _buffer[1] = buffer2;

        RandomAccess.Write(_file.SafeFileHandle, _buffer, _offset);
        _offset += buffer1.Length + buffer2.Length;
    }

    public void Flush() => RandomAccess.FlushToDisk(_file.SafeFileHandle);

    public ValueTask DisposeAsync() => _file.DisposeAsync();
}
