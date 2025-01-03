namespace JotDB.Configuration;

public sealed class WriteAheadLogOptions
{
    public string Mode { get; set; }
    public string Path { get; set; }
    public long Size { get; set; }
    public long BufferSize { get; set; }
}