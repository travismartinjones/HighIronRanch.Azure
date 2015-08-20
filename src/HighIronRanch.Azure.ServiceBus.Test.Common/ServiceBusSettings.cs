using System;
using System.IO;

namespace HighIronRanch.Azure.ServiceBus.Test.Common
{
	public class ServiceBusSettings : IServiceBusSettings
	{
		public string AzureServiceBusConnectionString { get; set; }
		public string ServiceBusSubscriptionNamePrefix { get; set; }

		public static ServiceBusSettings Create()
		{
			string[] settingsLines;

			try
			{
				settingsLines = File.ReadAllLines(Globals.SettingsFile);
			}
			catch (Exception ex)
			{
				throw new Exception(" Could not read settings file", ex);
			}

			var settings = new ServiceBusSettings()
			{
				AzureServiceBusConnectionString = settingsLines[0],
				ServiceBusSubscriptionNamePrefix = "TestPrefix"
			};

			return settings;
		}
	}
}