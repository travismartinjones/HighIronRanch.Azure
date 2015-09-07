using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;

namespace HighIronRanch.Azure.DocumentDb
{
    public interface IDocumentDbClientFactory
    {
        Task<DocumentClient> GetClientAsync(IDocumentDbSettings settings);
    }
}