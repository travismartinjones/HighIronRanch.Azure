using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using HighIronRanch.Core.Services;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace HighIronRanch.Azure.DocumentDb
{
    /// <summary>
    /// Make this a singleton.
    /// </summary>
    public class DocumentDbClientFactory : IDocumentDbClientFactory
    {
        private readonly ILogger _logger;

        private readonly IDictionary<string, DocumentClient> _clients = new ConcurrentDictionary<string, DocumentClient>();

        public DocumentDbClientFactory(ILogger logger)
        {
            _logger = logger;
        }

        public async Task<DocumentClient> GetClientAsync(IDocumentDbSettings settings)
        {
            var key = settings.DocumentDbRepositoryEndpointUrl + settings.DocumentDbRepositoryDatabaseId;
            if (!_clients.ContainsKey(key))
            {
                _logger.Debug(Common.LoggerContext, "Creating DocumentDb Client for {0}", settings.DocumentDbRepositoryEndpointUrl);

                var client = new DocumentClient(new Uri(settings.DocumentDbRepositoryEndpointUrl), settings.DocumentDbRepositoryAuthKey);
                await client.OpenAsync().ConfigureAwait(false);

                await SpinUpDatabaseAsync(client, settings.DocumentDbRepositoryDatabaseId).ConfigureAwait(false);

                _clients[key] = client;
                return client;
            }
            return _clients[key];
        }

        private async Task SpinUpDatabaseAsync(DocumentClient client, string databaseId)
        {
            var x = client.CreateDatabaseQuery()
                .Where(d => d.Id == databaseId)
                .AsEnumerable()
                .FirstOrDefault();
            if (x == null)
            {
                _logger.Debug(Common.LoggerContext, "Create DocumentDb database for {0}", databaseId);

                await client.CreateDatabaseAsync(new Database { Id = databaseId }).ConfigureAwait(false);
            }

/*
            var dbUri = UriFactory.CreateDatabaseUri(databaseId);
            var response = await client.ReadDatabaseAsync(dbUri);
            if (response.StatusCode != HttpStatusCode.OK)
            {
                _logger.Debug(Common.LoggerContext, "Create DocumentDb database for {0}", databaseId);

                await client.CreateDatabaseAsync(new Database { Id = databaseId });
            }
*/
        }
    }
}