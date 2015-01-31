using System;
using System.Runtime.Remoting.Messaging;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace HighIronRanch.Azure
{
	public interface ILeaseManager<in T>
	{
		//Task<bool> CreateLeaseObjectIfNotExist(T key);
		Task<string> Lease(T key);
		Task<string> Lease(T key, TimeSpan leaseTime);
		Task Release(T key, string leaseId);
		Task Renew(T key, string leaseId);
	}

	public class LeaseManager<T> : ILeaseManager<T>
	{
		private const int DefaultLeaseSeconds = 60;

		private readonly CloudBlobContainer _container;

		public LeaseManager(IAppSettings settings)
		{
			var leaseBlockName = settings.LeaseBlockName;
			if (!settings.IsLiveEnvironment)
				leaseBlockName = "dev" + leaseBlockName;

			var storageAccount = CloudStorageAccount.Parse(settings.AzureStorageConnectionString);
			var client = storageAccount.CreateCloudBlobClient();
			_container = client.GetContainerReference(leaseBlockName);
			_container.CreateIfNotExists();
		}

		public async Task<bool> CreateLeaseObjectIfNotExist(T key)
		{
			var leaseName = GetLeaseName(key);
			var blob = _container.GetBlockBlobReference(leaseName);
			if (!(await blob.ExistsAsync()))
			{
				await blob.UploadTextAsync("");
				return true;
			}
			return false;
		}

		public async Task<string> Lease(T key)
		{
			return await Lease(key, TimeSpan.FromSeconds(DefaultLeaseSeconds));
		}

		public async Task<string> Lease(T key, TimeSpan leaseTime)
		{
			var leaseName = GetLeaseName(key);
			var blob = _container.GetBlockBlobReference(leaseName);
			try
			{
				if (!(await blob.ExistsAsync()))
				{
					await blob.UploadTextAsync("");
				}

				var leaseId = await blob.AcquireLeaseAsync(leaseTime, Guid.NewGuid().ToString());
				return leaseId;
			}
			catch (StorageException ex)
			{
				if (ex.RequestInformation.HttpStatusCode == 400)
				{
					throw new UnableToAcquireLease("Unable to acquire lease", ex);
				}
				throw;
			}
		}

		public async Task Release(T key, string leaseId)
		{
			var leaseName = GetLeaseName(key);
			var blob = _container.GetBlockBlobReference(leaseName);
			await blob.ReleaseLeaseAsync(new AccessCondition
			{
				LeaseId = leaseId
			});
		}

		public async Task Renew(T key, string leaseId)
		{
			var blob = _container.GetBlockBlobReference(GetLeaseName(key));
			await blob.RenewLeaseAsync(new AccessCondition
			{
				LeaseId = leaseId
			});
		}

		private static string GetLeaseName(T key)
		{
			var leaseName = String.Format("{0}.lck", key);
			return leaseName;
		}

		public class UnableToAcquireLease : Exception
		{
			public UnableToAcquireLease(string message, Exception innerException)
				: base(message, innerException)
			{
			}
		}
	}
}