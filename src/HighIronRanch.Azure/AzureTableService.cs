using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table.Queryable;

namespace HighIronRanch.Azure
{
	public class AzureTableService : IAzureTableService
	{
		private readonly string _connectionString;

		public AzureTableService(IAppSettings appSettings)
		{
			_connectionString = appSettings.AzureStorageConnectionString;
		}

		protected string CleanseTableName(string uncleanTableName)
		{
			return Regex.Replace(uncleanTableName, @"[^a-zA-Z0-9]", "");
		}

		public CloudTable GetTable(string tableName, bool shouldCreateIfNotExists)
		{
			var client = CloudStorageAccount.Parse(_connectionString).CreateCloudTableClient();
			client.PayloadFormat = TablePayloadFormat.AtomPub;

			var cleansedTableName = CleanseTableName(tableName);
			
			var table = client.GetTableReference(cleansedTableName);
			if(shouldCreateIfNotExists)
				table.CreateIfNotExists();
			return table;
		}

		public CloudTable GetTable(string tableName)
		{
			return GetTable(tableName, true);
		}

	}
}