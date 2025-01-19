using System.Runtime.CompilerServices;
using JotDB.Memory;

namespace JotDB.Storage.Documents;

public sealed class DocumentCollection
{
    private readonly DocumentCollectionFile _file;

    private readonly AlignedMemory _buffer = AlignedMemory.Allocate(Capacity
        .UIntPtr.Mebibytes(4));

    public DocumentCollection(
        string collectionName)
    {
        CollectionName = collectionName;

        if (DocumentCollectionFile.Exists(collectionName))
        {
            _file = DocumentCollectionFile.Open(collectionName);

            InitializeExisting();
        }

        else
        {
            _file = DocumentCollectionFile.Create(
                collectionName,
                Capacity.Mebibytes(1));

            InitializeNew();
        }
    }

    public string CollectionName { get; }

    public DocumentCollectionHeader Header { get; private set; }

    private void InitializeExisting()
    {
        var size = Unsafe.SizeOf<DocumentCollectionHeader>();
        Span<byte> buffer = stackalloc byte[size];

        _file.Read(buffer, 0);

        Header = Unsafe.As<byte, DocumentCollectionHeader>(ref buffer[0]);
    }

    private unsafe void InitializeNew()
    {
        var writer = new AlignedMemoryWriter(_buffer);

        DocumentCollectionHeader header;
        var p = &header;

        p->MagicNumber = 0x8011ACFB;
        p->Version = 1;
        p->Id = Guid.NewGuid();

        writer.Write(*p);
        _file.Write(writer.AlignedSpan, 0);

        Header = header;
    }
}