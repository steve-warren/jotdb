namespace JotDB.Storage.Documents;

public sealed class DocumentCollection
{
    private readonly DocumentCollectionFile _file;

    public DocumentCollection(
        string collectionName)
    {
        CollectionName = collectionName;
        _file = DocumentCollectionFile.Open();
    }

    public string CollectionName { get; }
}