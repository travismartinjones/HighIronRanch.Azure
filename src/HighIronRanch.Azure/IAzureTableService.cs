using Microsoft.WindowsAzure.Storage.Table;

namespace HighIronRanch.Azure
{
	public interface IAzureTableService
	{
		CloudTable GetTable(string tableName, bool shouldCreateIfNotExists);
		CloudTable GetTable(string tableName);
	}
}