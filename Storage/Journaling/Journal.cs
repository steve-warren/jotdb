using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace JotDB.Storage.Journaling;

public sealed class Journal : IDisposable
{
    private readonly SafeFileHandle _file;
    private ulong _identity;
    private long _offset;

    public static Journal Open(
        string path)
    {
        using var file = File.Open(path, new FileStreamOptions
        {
            Access = FileAccess.Read,
            Mode = FileMode.OpenOrCreate,
            Share = FileShare.ReadWrite
        });

        return new Journal(
            path: path,
            offset: file.Length);
    }

    private Journal(
        string path,
        long offset)
    {
        Path = path;
        _offset = offset;

        _file = File.OpenHandle(
            path: path,
            mode: FileMode.Append,
            access: FileAccess.Write,
            share: FileShare.Read);
    }

    public string Path { get; }

    public void WriteToDisk(
        ReadOnlySpan<DocumentOperation> documentOperations)
    {
        var buffers = new ReadOnlyMemory<byte>[documentOperations.Length * 2];
        var rented = new List<byte[]>();
        var j = 0;

        for (var i = 0; i < documentOperations.Length; i++, j++)
        {
            var documentOperation = documentOperations[i];

            documentOperation.AssignOperationId(++_identity);

            var buffer = ArrayPool<byte>.Shared.Rent(13);
            rented.Add(buffer);

            var span = buffer.AsSpan(0, 13);

            SerializeJournalEntry(documentOperation, span);

            buffers[j] = buffer.AsMemory(0, 13);
            j++;
            buffers[j] = documentOperation.Data;
        }

        RandomAccess.Write(_file, buffers, _offset);

        foreach (var entry in documentOperations)
        {
            entry.CompleteWriteToJournal();
            _offset += 13 + entry.Data.Length;
        }

        foreach (var buffer in rented)
            ArrayPool<byte>.Shared.Return(buffer);
    }

    public void Dispose()
    {
        _file.Dispose();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void SerializeJournalEntry(
        DocumentOperation entry,
        Span<byte> buffer)
    {
        MemoryMarshal.Write(buffer[..8], entry.OperationId);
        MemoryMarshal.Write(buffer[8..12], entry.Data.Length);
        MemoryMarshal.Write(buffer[12..], entry.OperationType);
    }
}