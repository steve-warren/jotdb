namespace JotDB;

public class AppendOnlyFile
{
    private readonly FileStream _file;
    private long _offset;

    public AppendOnlyFile(FileStream file)
    {
        _file = file;
    }

    public void Write(ReadOnlySpan<byte> buffer)
    {
        RandomAccess.Write(_file.SafeFileHandle, buffer, _offset);
        _offset += buffer.Length;
    }

    public void Flush()
    {
        RandomAccess.FlushToDisk(_file.SafeFileHandle);
    }
}
