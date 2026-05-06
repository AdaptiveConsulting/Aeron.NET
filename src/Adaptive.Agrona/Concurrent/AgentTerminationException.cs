using System;

namespace Adaptive.Agrona.Concurrent
{
    public class AgentTerminationException : Exception
    {
        public AgentTerminationException()
        {
        }

        public AgentTerminationException(string message) : base(message)
        {
        }

        public AgentTerminationException(Exception innerException) : base(innerException?.ToString(), innerException)
        {
        }
    }
}