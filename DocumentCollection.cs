using System.Diagnostics;
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

      using var fs = File.OpenHandle(
         "documents.jotdb",
         FileMode.OpenOrCreate,
         FileAccess.ReadWrite,
         FileShare.Read, FileOptions.Asynchronous);

      long offset = 0;
      var watch = new Stopwatch();

      await foreach (var entry in reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
      {
         if (cancellationToken.IsCancellationRequested)
            break;

         watch.Restart();
         await RandomAccess.WriteAsync(fs, entry.Data, offset, cancellationToken).ConfigureAwait(false);

         Console.WriteLine($"data {entry.Identity} written in {watch.ElapsedMilliseconds}ms");

         offset += entry.Data.Length;
      }

      Console.WriteLine("Pending document write processor canceled.");
   }
}