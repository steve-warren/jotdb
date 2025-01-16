namespace JotDB.Storage.Journal;

public sealed class NullWriteAheadLogFile : WriteAheadLogFile
{
    public override void Flush()
    {
        // no-op
    }

    public override void Write(ReadOnlySpan<byte> span)
    {
        // no-op
    }

    public override bool Rotate()
    {
        return false;
    }

    public override void Dispose()
    {
        // no-op
    }
}