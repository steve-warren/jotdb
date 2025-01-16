using System.Diagnostics;
using System.Runtime.InteropServices;
using JotDB.Metrics;
using JotDB.Platform.MacOS;
using JotDB.Platform.Windows;
using JotDB.Storage.Documents;
using Microsoft.Win32.SafeHandles;

namespace JotDB.Storage.Journal;

public sealed class SafeFileHandleWriteAheadLogFile : WriteAheadLogFile
{
    private static readonly int MAX_FILE_SIZE = Capacity.Int32.Mebibytes(4);

    private SafeFileHandle _fileHandle;

    public static SafeFileHandleWriteAheadLogFile Open()
    {
        return new SafeFileHandleWriteAheadLogFile(
            offset: 0);
    }

    private static SafeFileHandle OpenFileHandle()
    {
        var path = $"jotdb_{DateTime
            .Now:yyyyMMddHHmmssff}.wal";

        if (OperatingSystem.IsWindows())
            return WindowsWriteAheadLogFileInterop.OpenFileHandle(path, MAX_FILE_SIZE);

        else if (OperatingSystem.IsMacOS())
            return MacWriteAheadLogFileInterop.OpenFileHandle(path, MAX_FILE_SIZE);

        else
            throw new PlatformNotSupportedException();
    }

    private SafeFileHandleWriteAheadLogFile(
        long offset)
    {
        Offset = offset;
        _fileHandle = OpenFileHandle();
    }

    public long Offset { get; private set; }

    public override bool Rotate()
    {
        if (Offset < MAX_FILE_SIZE) return false;

        // todo: we could perform the rotation asynchronously

        var watch = StopwatchSlim.StartNew();

        var newHandle = OpenFileHandle();

        Offset = 0;
        var oldHandle = _fileHandle;
        _fileHandle = newHandle;
        RandomAccess.FlushToDisk(oldHandle);
        oldHandle.Dispose();

        MetricSink.WriteAheadLog.Rotate(watch.Elapsed);

        return true;
    }

    public override void Dispose()
    {
        _fileHandle.Dispose();
    }

    public override void Flush() => RandomAccess.FlushToDisk(_fileHandle);

    public override void Write(ReadOnlySpan<byte> span)
    {
        var watch = StopwatchSlim.StartNew();
        RandomAccess.Write(_fileHandle, span, Offset);
        MetricSink.WriteAheadLog.Write(watch.Elapsed);
        Offset += span.Length;
    }
}