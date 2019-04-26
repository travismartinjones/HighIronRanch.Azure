using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace HighIronRanch.Azure.TableStorage
{
	public interface IAzureTableService
	{
		Task<CloudTable> GetTable(string tableName, bool shouldCreateIfNotExists);
		Task<CloudTable> GetTable(string tableName);
	}
}