using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Machine.Specifications;

namespace HighIronRanch.Azure.TableStorage.Test.Integration
{
    [Subject(typeof(TestableAzureTableService))]
    public class AzureTableServiceSpecs
    {
        public class AppSettings : IAzureTableSettings
        {
            protected static string ConnectionString = "UseDevelopmentStorage=true;";
            public string AzureStorageConnectionString => ConnectionString;
        }

        public class when_getting_a_table
        {
            private static TestableAzureTableService sut;

            private static string _testTableName;

            private Establish context = () =>
            {
                var appSettings = new AppSettings();
                _testTableName = "TESTTABLE" + DateTime.Now.Millisecond.ToString(CultureInfo.InvariantCulture);
                sut = new TestableAzureTableService(appSettings);
            };

            private Because of = () => sut.GetTable(_testTableName);

            private It should_create_the_table = () => sut.DoesTableExist(_testTableName).ShouldBeTrue();

            private Cleanup after = () =>
            {
                var table = sut.GetTable(_testTableName, false);
                table.DeleteIfExists();
            };
        }
    }

    public class TestableAzureTableService : AzureTableService
    {
        public TestableAzureTableService(IAzureTableSettings appSettings) : base(appSettings)
        {
        }

        public bool DoesTableExist(string tableName)
        {
            var client = GetClient();

            var cleansedTableName = CleanseTableName(tableName);

            var table = client.GetTableReference(cleansedTableName);
            return table.Exists();
        }
    }

}
