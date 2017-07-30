using System;
using System.Threading;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// <see cref="Agent"/> container which does not start a thread. It instead allows the duty ctyle <see cref="IAgent.DoWork"/> to be
    /// invoked directly.
    /// 
    /// Exceptions which occur during the <see cref="IAgent.DoWork"/> invocation will be caught and passed to the provided <see cref="ErrorHandler"/>.
    /// 
    /// <b>Note: </b> This class is not threadsafe.
    /// 
    /// </summary>
    public class AgentInvoker : IDisposable
    {
        private bool _closed = false;

        private readonly AtomicCounter _errorCounter;
        private readonly ErrorHandler _errorHandler;
        private readonly IAgent _agent;

        public AgentInvoker(
            ErrorHandler errorHandler,
            AtomicCounter errorCounter,
            IAgent agent
        )
        {
            Objects.RequireNonNull(errorHandler, "errorHandler");
            Objects.RequireNonNull(agent, "agent");

            _errorHandler = errorHandler;
            _errorCounter = errorCounter;
            _agent = agent;
        }

        /// <summary>
        /// The <see cref="Agent"/> which is contained.
        /// </summary>
        /// <returns> <see cref="Agent"/> being contained.</returns>
        public IAgent Agent()
        {
            return _agent;
        }

        public int Invoke()
        {
            int workCount = 0;

            if (!_closed)
            {
                try
                {
                    workCount = _agent.DoWork();
                }
                catch (ThreadInterruptedException)
                {

                }
                catch (Exception exception)
                {
                    if (null != _errorCounter)
                    {
                        _errorCounter.Increment();
                    }

                    _errorHandler(exception);
                }
            }

            return workCount;
        }

        public void Dispose()
        {
            if (!_closed)
            {
                _closed = true;
                _agent.OnClose();
            }
        }
    }
}