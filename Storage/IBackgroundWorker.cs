namespace JotDB.Storage;

public interface IBackgroundWorker
{
    void Start();
    Task StopAsync();
}