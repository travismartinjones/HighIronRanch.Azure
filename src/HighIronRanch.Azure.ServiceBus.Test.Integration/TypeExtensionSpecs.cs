using Machine.Specifications;

namespace HighIronRanch.Azure.ServiceBus.Test.Integration
{
	public class TypeExtensionSpecs
	{
		[Subject(typeof (TypeExtensions))]
		public class When_checking_a_type_that_implements_a_simple_interface
		{
			private It should_implement_the_interface = () =>
				typeof(TestClass)
					.DoesTypeImplementInterface(typeof(ITestInterface))
					.ShouldEqual(true);
		}

		[Subject(typeof(TypeExtensions))]
		public class When_checking_a_type_that_implements_a_generic_interface
		{
			private It should_implement_the_interface = () =>
				typeof(TestGenericClass<int>)
					.DoesTypeImplementInterface(typeof(ITestGenericInterface<>))
					.ShouldEqual(true);
		}

		[Subject(typeof(TypeExtensions))]
		public class When_checking_a_type_that_is_an_interface
		{
			private It should_not_implement_the_interface = () =>
				typeof(ITestInterface)
					.DoesTypeImplementInterface(typeof(ITestInterface))
					.ShouldEqual(false);
		}

		private interface ITestInterface { }
		private class TestClass : ITestInterface { }

		private interface ITestGenericInterface<T> { }
		private class TestGenericClass<T> : ITestGenericInterface<T> { }
	}
}