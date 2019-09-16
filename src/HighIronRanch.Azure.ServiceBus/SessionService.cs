using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;
using Microsoft.ServiceBus.Messaging;

namespace HighIronRanch.Azure.ServiceBus
{
    public class SessionService : ISessionService
    {
        public static ConcurrentDictionary<string, MessageSession> _sessions = new ConcurrentDictionary<string, MessageSession>();

        public void Add(MessageSession session)
        {
            if (session == null) return;
            _sessions.AddOrUpdate(session.SessionId, s => session, (s, messageSession) => session);
        }

        public void Remove(MessageSession session)
        {
            if (session == null) return;
            _sessions.TryRemove(session.SessionId, out session);
        }

        public async Task ClearAll()
        {
            var closeSessionTasks = _sessions.Values.Select(x => x.CloseAsync());
            await Task.WhenAll(closeSessionTasks).ConfigureAwait(false);
        }
    }
}