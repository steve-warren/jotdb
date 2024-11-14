using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace JotDB;

public sealed class Journal : IDisposable
{
    private readonly SafeFileHandle _file;
    private ulong _identity;
    private long _offset;

    public static Journal Open(string path)
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
            share: FileShare.Read,
            FileOptions.WriteThrough);
    }

    public string Path { get; }

    public void WriteToDisk(ReadOnlySpan<JournalEntry> entries)
    {
        var buffers = new ReadOnlyMemory<byte>[entries.Length * 2];
        var rented = new List<byte[]>();
        var j = 0;

        for (var i = 0; i < entries.Length; i++, j++)
        {
            var entry = entries[i];

            entry.AssignIdentity(++_identity);

            var buffer = ArrayPool<byte>.Shared.Rent(13);
            rented.Add(buffer);

            var span = buffer.AsSpan(0, 13);

            SerializeJournalEntry(entry, span);

            buffers[j] = buffer.AsMemory(0, 13);
            j++;
            buffers[j] = entry.Data;
        }

        RandomAccess.Write(_file, buffers, _offset);

        foreach (var entry in entries)
        {
            entry.CompleteWriteToDisk();
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
        JournalEntry entry,
        Span<byte> buffer)
    {
        MemoryMarshal.Write(buffer[..8], entry.Identity);
        MemoryMarshal.Write(buffer[8..12], entry.Data.Length);
        MemoryMarshal.Write(buffer[12..], entry.Operation);
    }
}