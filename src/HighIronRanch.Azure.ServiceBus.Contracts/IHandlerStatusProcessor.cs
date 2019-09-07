using System;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
    public interface IHandlerStatusProcessor
    {
        void Begin(string handlerType, string id, DateTime eventDate);
        void Complete(string handlerType, string id, double elapsedSeconds);
        void Abandon(string handlerType, string id, Exception ex);
        void Error(string handlerType, string id, Exception ex);
        void BusComplete(string handlerType, string id, double elapsedSeconds);        
        void BusAbandon(string handlerType, string id, double elapsedSeconds);                
    }
}