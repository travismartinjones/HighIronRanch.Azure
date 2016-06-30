using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Configuration;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using developwithpassion.specifications.rhinomocks;
using HighIronRanch.Azure.ServiceBus.Test.Common;
using Machine.Specifications;
using Microsoft.ServiceBus.Messaging;

namespace HighIronRanch.Azure.ServiceBus.Test.Integration
{
	public class ServiceBusSpecs
    {
	    [Subject(typeof (ServiceBus))]
	    public class Concern : Observes<ServiceBus>
	    {
		    public static readonly string TestQueueName = "TestQueueName";

			private static ServiceBusSettings _settings;

			private Establish ee = () =>
			{
				_settings = ServiceBusSettings.Create();

				depends.on<IServiceBusSettings>(_settings);

				var nsManager = new NamespaceManagerBuilder();
				depends.on<INamespaceManagerBuilder>(nsManager);
			};
		}

		public class CleaningConcern : Concern
		{
			private Cleanup cc = () =>
			{
				var task = sut.DeleteQueueAsync(TestQueueName);
				task.Wait();
			};
		}

	    public class When_sending_a_message_to_a_queue : CleaningConcern
	    {
		    private static string testBody = "Test";
		    private static BrokeredMessage msg;

		    private Because of = () =>
		    {
			    var task = sut.CreateQueueAsync(TestQueueName, false);
			    task.Wait();

			    var clientTask = sut.CreateQueueClientAsync(TestQueueName, false);
			    clientTask.Wait();
			    var client = clientTask.Result;

				client.Send(new BrokeredMessage(testBody));

			    msg = client.Receive();
		    };

			private It should_receive_the_sent_message = () => msg.GetBody<string>().ShouldEqual(testBody);
	    }

	    public class When_processing_a_message_for_too_long : CleaningConcern
	    {
		    private static Type exceptionType;

		    private Because of = () =>
		    {
			    var task = sut.CreateQueueAsync(TestQueueName, false);
			    task.Wait();

			    var clientTask = sut.CreateQueueClientAsync(TestQueueName, false);
			    clientTask.Wait();
			    var client = clientTask.Result;

				client.Send(new BrokeredMessage("Howdy"));

			    try
			    {
					var msg = client.Receive();
					Thread.Sleep(65000);
					msg.Complete();
			    }
			    catch (Exception ex)
			    {
				    exceptionType = ex.GetType();
			    }
		    };

		    private It should_lose_the_message_lock = () => exceptionType.ShouldEqual(typeof(MessageLockLostException));
	    }

	    public class When_renewing_a_message_lock : CleaningConcern
	    {
		    private static bool isComplete = false;

			private Because of = () =>
			{
				var task = sut.CreateQueueAsync(TestQueueName, false);
				task.Wait();

				var clientTask = sut.CreateQueueClientAsync(TestQueueName, false);
				clientTask.Wait();
				var client = clientTask.Result;

				client.Send(new BrokeredMessage("Howdy"));

				var msg = client.Receive();
				Thread.Sleep(55000);
				msg.RenewLock();
				Thread.Sleep(10000);
				msg.Complete();
				isComplete = true;
			};

			private It should_complete_the_message = () => isComplete.ShouldBeTrue();
		}

		public class TestableServiceBus : ServiceBus
		{
			public TestableServiceBus(IServiceBusSettings settings, INamespaceManagerBuilder managerBuilder
#if USE_MESSAGING_FACTORY
                , IMessagingFactoryBuilder factoryBuilder
#endif
                ) : base(settings, managerBuilder
#if USE_MESSAGING_FACTORY
                    , factoryBuilder
#endif
                    )
			{
			}

			public static string TestableCleanseName(string name)
			{
				return CleanseName(name);
			}
		}

		[Subject(typeof(TestableServiceBus))]
		public class CleansingConcern : Observes<TestableServiceBus>
		{
			protected static string _result;
			protected static string _allValidCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ.01234-56789_abcdefghijklmnopqrstuvwxyz";
			protected static string _someInvalidCharacters = "ABCDEFGHIJK+LMNOPQRS#TUVWXYZ.01234-56789_abcdefgh?ijklmno[pqrstuvw!xyz";
			protected static string _someCleansedCharacters = "ABCDEFGHIJK_LMNOPQRS_TUVWXYZ.01234-56789_abcdefgh_ijklmno_pqrstuvw_xyz";

			private Establish ee = () => sut_factory.create_using(() => null);
		}

		public class When_cleansing_a_name_with_all_valid_characters : CleansingConcern
		{
			Because of = () => _result = TestableServiceBus.TestableCleanseName(_allValidCharacters);

			private It should_not_replace_valid_characters = () => _result.ShouldEqual(_allValidCharacters);
		}

		public class When_cleansing_a_name_with_some_invalid_characters : CleansingConcern
		{
			Because of = () => _result = TestableServiceBus.TestableCleanseName(_someInvalidCharacters);

			private It should_replace_invalid_characters = () => _result.ShouldEqual(_someCleansedCharacters);
		}
	}
}
