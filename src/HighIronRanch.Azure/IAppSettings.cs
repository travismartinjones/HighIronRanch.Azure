namespace HighIronRanch.Azure
{
	public interface IAppSettings
	{
		bool IsLiveEnvironment { get; }
		string AzureStorageConnectionString { get; }
		string LeaseBlockName { get; }
		string DevLucenePath { get; }
	}
}