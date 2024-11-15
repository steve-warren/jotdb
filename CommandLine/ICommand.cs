namespace JotDB.CommandLine;

public interface ICommand
{
    Task ExecuteAsync(CancellationToken cancellationToken = default);
}