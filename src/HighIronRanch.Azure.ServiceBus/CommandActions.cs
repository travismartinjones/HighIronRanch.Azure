using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;

namespace HighIronRanch.Azure.ServiceBus.Standard
{
    public class CommandActions : ICommandActions
    {
        private readonly Func<Task> _renewLock;

        public CommandActions(Func<Task> renewLock)
        {
            _renewLock = renewLock;            
        }

        public async Task RenewLockAsync()
        {
            await _renewLock.Invoke().ConfigureAwait(false);
        }
    }
}
