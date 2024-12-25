namespace JotDB.Storage.Journal;

public abstract class WriteAheadLogFile : IDisposable
{
    public abstract void Flush();

    public abstract void Write(
        ReadOnlySpan<byte> span);

    public abstract bool Rotate();

    public abstract void Dispose();
}