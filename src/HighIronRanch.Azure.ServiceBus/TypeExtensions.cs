using System;
using System.Linq;

namespace HighIronRanch.Azure.ServiceBus
{
	internal static class TypeExtensions
	{
		public static bool DoesTypeImplementInterface(this Type type, Type @interface)
		{
			if (type.IsAbstract || type.IsInterface)
				return false;

			// This doesn't work: typeof(IMessageHandler<>).IsAssignableFrom(typeof(TestMessageHandler))
			//return @interface.IsAssignableFrom(type);

			// But this does:
			return type
				.GetInterfaces()
				.Any(x => x.IsGenericType && x.GetGenericTypeDefinition() == @interface);
		}
	}
}