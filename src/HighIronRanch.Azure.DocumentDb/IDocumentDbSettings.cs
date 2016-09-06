namespace HighIronRanch.Azure.DocumentDb
{
    public interface IDocumentDbSettings
    {
        string DocumentDbRepositoryEndpointUrl { get; }
        string DocumentDbRepositoryAuthKey { get; }
        string DocumentDbRepositoryDatabaseId { get; }
    }
}