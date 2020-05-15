using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;

namespace HighIronRanch.Azure.TableStorage
{
	public interface IAzureTableService
	{
		Task<CloudTable> GetTable(string tableName, bool shouldCreateIfNotExists);
		Task<CloudTable> GetTable(string tableName);
	}
}