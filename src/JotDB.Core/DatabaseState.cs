namespace JotDB;

public enum DatabaseState : byte
{
    Stopped,
    Starting,
    Running,
    Stopping
}