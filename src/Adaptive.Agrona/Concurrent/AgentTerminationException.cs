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
    }
}