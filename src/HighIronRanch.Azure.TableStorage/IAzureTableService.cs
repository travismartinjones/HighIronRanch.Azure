using Microsoft.WindowsAzure.Storage.Table;

namespace HighIronRanch.Azure.TableStorage
{
	public interface IAzureTableService
	{
		CloudTable GetTable(string tableName, bool shouldCreateIfNotExists);
		CloudTable GetTable(string tableName);
	}
}