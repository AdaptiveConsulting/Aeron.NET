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
        /// <summary>
        /// Has the <see cref="IAgent"/> been closed?
        /// </summary>
        public bool IsClosed { get; private set; } = false;

        /// <summary>
        /// Has the <see cref="IAgent"/> been started?
        /// </summary>
        public bool IsStarted { get; private set; } = false;

        /// <summary>
        /// Has the <see cref="IAgent"/> been running?
        /// </summary>
        public bool IsRunning { get; private set; } = false;

        private readonly AtomicCounter _errorCounter;
        private readonly ErrorHandler _errorHandler;
        private readonly IAgent _agent;

        /// <summary>
        /// Create an agent and initialise it.
        /// </summary>
        /// <param name="errorHandler"> to be called if an <seealso cref="Exception"/> is encountered </param>
        /// <param name="errorCounter"> to be incremented each time an exception is encountered. This may be null. </param>
        /// <param name="agent">        to be run in this thread. </param>
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

        /// <summary>
        /// Mark the invoker as started and call the <seealso cref="IAgent.OnStart"/> method.
        /// <para>
        /// Startup logic will only be performed once.
        /// </para>
        /// </summary>
        public void Start()
        {
            try
            {
                if (!IsStarted)
                {
                    IsStarted = true;
                    _agent.OnStart();
                    IsRunning = true;
                }
            }
            catch (Exception exception)
            {
                HandleError(exception);
                Dispose();
            }
        }

        /// <summary>
        /// Invoke the <seealso cref="IAgent.DoWork()"/> method and return the work count.
        /// 
        /// If an error occurs then the <seealso cref="AtomicCounter.Increment"/> will be called on the errorCounter if not null
        /// and the <seealso cref="Exception"/> will be passed to the <seealso cref="ErrorHandler"/> method. If the error
        /// is an <seealso cref="AgentTerminationException"/> then <seealso cref="Dispose"/> will be called after the error handler.
        /// 
        /// If not successfully started or after closed then this method will return without invoking the <seealso cref="Agent"/>.
        ///     
        /// </summary>
        /// <returns> the work count for the <seealso cref="IAgent.DoWork"/> method. </returns>
        public int Invoke()
        {
            int workCount = 0;

            if (IsRunning)
            {
                try
                {
                    workCount = _agent.DoWork();
                }
                catch (ThreadInterruptedException)
                {
                    Dispose();
                }
                catch (AgentTerminationException ex)
                {
                    HandleError(ex);
                    Dispose();
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
            try
            {
                if (!IsClosed)
                {
                    IsRunning = false;
                    IsClosed = true;
                    _agent.OnClose();
                }
            }
            catch (Exception exception)
            {
                HandleError(exception);
            }

        }

        private void HandleError(Exception exception)
        {
            if (null != _errorCounter)
            {
                _errorCounter.Increment();
            }

            _errorHandler(exception);
        }
    }
}