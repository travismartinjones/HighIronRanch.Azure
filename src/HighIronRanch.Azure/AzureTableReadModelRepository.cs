using System;
using System.Collections.Generic;
using System.Linq;
using HighIronRanch.Core;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace HighIronRanch.Azure
{
/*
	public class AzureTableReadModelRepository : IReadModelRepository
	{
		public const string READ_MODEL_TABLE_NAME_PREFIX = "ReadModel";

		private readonly IAzureTableService _tableService;
		protected string _readModelTableNamePrefix; // Used so it can be overridden for tests

		public AzureTableReadModelRepository(IAzureTableService tableService)
		{
			_tableService = tableService;
			_readModelTableNamePrefix = READ_MODEL_TABLE_NAME_PREFIX;
		}

		protected class AzureReadModel : TableEntity
		{
			public string ModelAsJson { get; set; }

			public AzureReadModel() { }

			public AzureReadModel(IReadModel model)
			{
				PartitionKey = model.Id.ToString();
				RowKey = model.ToString();
				ModelAsJson = JsonConvert.SerializeObject(model);
			}
		}

		public void Save<T>(T item) where T : IReadModel
		{
			var table = _tableService.GetTable(_readModelTableNamePrefix + typeof(T).ToString());

			var model = new AzureReadModel(item);
			var tableOperation = TableOperation.InsertOrReplace(model);

			table.Execute(tableOperation);
		}

		public IEnumerable<T> Get<T>() where T : IReadModel, new()
		{
			var table = _tableService.GetTable(_readModelTableNamePrefix + typeof(T).ToString());

			var query = new TableQuery<AzureReadModel>();
			var models = table.ExecuteQuery(query);

			return models.Select(model => JsonConvert.DeserializeObject<T>(model.ModelAsJson));
		}

		public IEnumerable<object> Get(Type type)
		{
			var table = _tableService.GetTable(_readModelTableNamePrefix + type.ToString());

			var query = new TableQuery<AzureReadModel>();
			var models = table.ExecuteQuery(query);

			return models.Select(model => JsonConvert.DeserializeObject(model.ModelAsJson, type));
		}

		public T GetById<T>(Guid id) where T : IReadModel, new()
		{
			var table = _tableService.GetTable(_readModelTableNamePrefix + typeof(T).ToString());

			var query = new TableQuery<AzureReadModel>()
				.Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, id.ToString()));

			var readModels = table.ExecuteQuery(query);
			if (!readModels.Any())
				return default(T);
			return JsonConvert.DeserializeObject<T>(readModels.First().ModelAsJson);
		}
	}
*/
}