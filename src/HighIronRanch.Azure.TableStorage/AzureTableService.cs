using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace HighIronRanch.Azure.TableStorage
{
	public interface IAzureTableSettings
	{
		string AzureStorageConnectionString { get; }
	}

	public class AzureTableService : IAzureTableService
	{
		private readonly IAzureTableSettings _appSettings;

		public AzureTableService(IAzureTableSettings appSettings)
		{
			_appSettings = appSettings;
		}

		protected string CleanseTableName(string uncleanTableName)
		{
			return Regex.Replace(uncleanTableName, @"[^a-zA-Z0-9]", "");
		}

		public async Task<CloudTable> GetTable(string tableName, bool shouldCreateIfNotExists)
		{
			var client = CloudStorageAccount.Parse(_appSettings.AzureStorageConnectionString).CreateCloudTableClient();
			
			var cleansedTableName = CleanseTableName(tableName);
			
			var table = client.GetTableReference(cleansedTableName);
			if(shouldCreateIfNotExists)
				await table.CreateIfNotExistsAsync().ConfigureAwait(false);
			return table;
		}

		public async Task<CloudTable> GetTable(string tableName)
		{
			return await GetTable(tableName, true).ConfigureAwait(false);
		}

	}
}