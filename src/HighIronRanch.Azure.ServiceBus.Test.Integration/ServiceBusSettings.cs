using System;
using System.IO;

namespace HighIronRanch.Azure.ServiceBus.Test.Integration
{
	public class ServiceBusSettings : IServiceBusSettings
	{
		public string AzureServiceBusConnectionString { get; set; }

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
				AzureServiceBusConnectionString = settingsLines[0]
			};

			return settings;
		}
	}
}