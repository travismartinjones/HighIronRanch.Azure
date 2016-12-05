using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using HighIronRanch.Core;
using HighIronRanch.Core.Helpers;
using HighIronRanch.Core.Repositories;
using HighIronRanch.Core.Services;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;

namespace HighIronRanch.Azure.DocumentDb
{
    public class DocumentDbViewModelRepository : IViewModelRepository
    {
        protected readonly IDocumentDbSettings _settings;
        protected readonly IDocumentDbClientFactory _clientFactory;
        protected readonly ILogger _logger;

        protected readonly IDictionary<Type, Uri> _collectionUris = new Dictionary<Type, Uri>(); 

        public DocumentDbViewModelRepository(IDocumentDbSettings settings, IDocumentDbClientFactory clientFactory, ILogger logger)
        {
            _settings = settings;
            _clientFactory = clientFactory;
            _logger = logger;
        }

        public class CollectionDoesNotExist : Exception
        {
            public CollectionDoesNotExist(Type type) : base($"Collection {type.Name} does not exist")
            {
                
            }
        }

        /// <summary>
        /// Verifies the collection exists and throws an exception if it does not.
        /// Marked virtual so a subclass with writing functionality could create the collection.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        protected virtual async Task CreateCollectionIfNecessaryAsync<T>()
        {
            var databaseLink = UriFactory.CreateDatabaseUri(_settings.DocumentDbRepositoryDatabaseId);
            var client = await _clientFactory.GetClientAsync(_settings);
            var collection = client.CreateDocumentCollectionQuery(databaseLink)
                                .Where(c => c.Id == typeof(T).Name)
                                .AsEnumerable()
                                .FirstOrDefault();
            if (collection == null)
            {
                _logger.Error(Common.LoggerContext, "Collection {0} does not exist", typeof(T).Name);
                throw new CollectionDoesNotExist(typeof(T));
            }
        }

        protected async Task<Uri> GetCollectionLinkAsync<T>()
        {
            if (!_collectionUris.ContainsKey(typeof (T)))
            {
                await CreateCollectionIfNecessaryAsync<T>();

                _collectionUris[typeof(T)] = UriFactory.CreateCollectionUri(_settings.DocumentDbRepositoryDatabaseId, typeof(T).Name);
            }
            return _collectionUris[typeof(T)];
        }

        protected Uri GetDocumentLink<T>(string documentId)
        {
            return UriFactory.CreateDocumentUri(_settings.DocumentDbRepositoryDatabaseId, typeof (T).Name, documentId);
        }

        public async Task<IQueryable<T>> GetAsync<T>() where T : IViewModel, new()
        {
            try
            {
                var collectionLink = await GetCollectionLinkAsync<T>();
                var client = await _clientFactory.GetClientAsync(_settings);
                var queryable = client.CreateDocumentQuery<T>(collectionLink);
                return queryable;
            }
            catch (Exception)
            {
                return new List<T>().AsQueryable();
            }
        }

        public IQueryable<T> Get<T>() where T : IViewModel, new()
        {
            return AsyncHelpers.RunSync<IQueryable<T>>(() => GetAsync<T>());
        }

        public async Task<T> GetAsync<T>(Guid id) where T : IViewModel, new()
        {
            var documentLink = GetDocumentLink<T>(id.ToString());
            var client = await _clientFactory.GetClientAsync(_settings);
            var response = await client.ReadDocumentAsync(documentLink);
            return JsonConvert.DeserializeObject<T>(response.Resource.ToString());
        }

        public T Get<T>(Guid id) where T : IViewModel, new()
        {
            return AsyncHelpers.RunSync<T>(() => GetAsync<T>(id));
        }
    }

}
