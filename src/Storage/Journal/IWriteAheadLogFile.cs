using JotDB.Memory;

namespace JotDB.Storage.Journal;

public interface IWriteAheadLogFile
{
    void FlushToDisk();

    void WriteToDisk(
        ReadOnlySpan<byte> span);
}