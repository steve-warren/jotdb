using JotDB.Memory;

namespace JotDB.Storage;

public interface IWriteAheadLogFile
{
    void FlushToDisk();

    void WriteToDisk(
        AlignedMemory memory);
}