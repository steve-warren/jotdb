namespace JotDB.Storage;

public enum JournalEntryType : byte
{
    Insert = 1,
    Update = 2,
    Delete = 3
}