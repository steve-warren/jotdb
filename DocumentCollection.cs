using System.Threading.Channels;

namespace JotDB;

public class DocumentCollection
{
   private readonly Channel<JournalEntry> _journalEntries;

   public DocumentCollection(string path)
   {
      _journalEntries = Channel.CreateUnbounded<JournalEntry>(new UnboundedChannelOptions
      {
         SingleReader = true,
         SingleWriter = true,
         AllowSynchronousContinuations = true
      });
   }
 
   public ChannelWriter<JournalEntry> PendingDocumentWriteQueue => _journalEntries.Writer;

   public async Task ProcessPendingDocumentWriteOperationsAsync(
      CancellationToken cancellationToken)
   {
      var reader = _journalEntries.Reader;

      await foreach (var entry in reader.ReadAllAsync(CancellationToken.None).ConfigureAwait(false))
      {
         if (cancellationToken.IsCancellationRequested)
            break;

         Console.WriteLine($"writing entry {entry.Identity} to the data file.");
      }
      
      Console.WriteLine("Pending document write processor canceled.");
   }
}