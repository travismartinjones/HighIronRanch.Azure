using System;
using System.IO;

namespace HighIronRanch.Azure.DocumentDb.Test.Integration
{
    public class DocumentDbSettings : IDocumentDbSettings
    {
        public static readonly string SettingsFile = "c:\\temp\\HighIronRanch.Azure.DocumentDb.Test.Settings.txt";

        public string DocumentDbRepositoryEndpointUrl { get; set; }
        public string DocumentDbRepositoryAuthKey { get; set; }
        public string DocumentDbRepositoryDatabaseId { get; set; }

        public static DocumentDbSettings Create()
        {
            string[] settingsLines;

            try
            {
                settingsLines = File.ReadAllLines(SettingsFile);
            }
            catch (Exception ex)
            {
                throw new Exception(" Could not read settings file", ex);
            }

            var settings = new DocumentDbSettings()
            {
                DocumentDbRepositoryEndpointUrl = settingsLines[0],
                DocumentDbRepositoryAuthKey = settingsLines[1],
                DocumentDbRepositoryDatabaseId = "TestDatabase" + DateTime.Now.Ticks
            };

            return settings;
        }
    }
}
