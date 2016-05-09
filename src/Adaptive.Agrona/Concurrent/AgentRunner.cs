using System;
using System.Threading;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// Base agent runner that is responsible for lifecycle of an <seealso cref="Agent"/> and ensuring exceptions are handled.
    /// <para>
    /// Note: An agent runner should only be once per instance.
    /// </para>
    /// </summary>
    public class AgentRunner : IDisposable
    {
        private static readonly Thread Tombstone = null;

        private volatile bool _running = true;

        private readonly AtomicCounter _errorCounter;
        private readonly ErrorHandler _errorHandler;
        private readonly IIdleStrategy _idleStrategy;
        private readonly IAgent _agent;
        private readonly AtomicReference<Thread> _thread = new AtomicReference<Thread>();

        /// <summary>
        /// Create an agent passing in <seealso cref="_idleStrategy"/>
        /// </summary>
        /// <param name="idleStrategy"> to use for Agent run loop </param>
        /// <param name="errorHandler"> to be called if an <seealso cref="Exception"/> is encountered </param>
        /// <param name="errorCounter"> for reporting how many exceptions have been seen. </param>
        /// <param name="agent">        to be run in this thread. </param>
        public AgentRunner(IIdleStrategy idleStrategy, ErrorHandler errorHandler, AtomicCounter errorCounter, IAgent agent)
        {
            if (idleStrategy == null) throw new ArgumentNullException(nameof(idleStrategy));
            if (errorHandler == null) throw new ArgumentNullException(nameof(errorHandler));
            if (agent == null) throw new ArgumentNullException(nameof(agent));


            _idleStrategy = idleStrategy;
            _errorHandler = errorHandler;
            _errorCounter = errorCounter;
            _agent = agent;
        }

        /// <summary>
        /// Start the given agent runner on a new thread.
        /// </summary>
        /// <param name="runner"> the agent runner to start </param>
        public static void StartOnThread(AgentRunner runner)
        {
            var thread = new Thread(runner.Run)
            {
                Name = runner.Agent().RoleName()
            };
            thread.Start();
        }

        /// <summary>
        /// The <seealso cref="IAgent"/> who's lifecycle is being managed.
        /// </summary>
        /// <returns> <seealso cref="IAgent"/> who's lifecycle is being managed. </returns>
        public IAgent Agent()
        {
            return _agent;
        }

        /// <summary>
        /// Run an <seealso cref="IAgent"/>.
        /// <para>
        /// This method does not return until the run loop is stopped via <seealso cref="Dispose()"/>.
        /// </para>
        /// </summary>
        public void Run()
        {
            if (!_thread.CompareAndSet(null, Thread.CurrentThread))
            {
                return;
            }

            var idleStrategy = _idleStrategy;
            var agent = _agent;
            while (_running)
            {
                try
                {
                    idleStrategy.Idle(agent.DoWork());
                }
                catch (ThreadInterruptedException)
                {
                }
                catch (Exception ex)
                {
                    _errorCounter?.Increment();

                    _errorHandler(ex);
                }
            }
        }

        /// <summary>
        /// Stop the running Agent and cleanup. This will wait for the work loop to exit and the <seealso cref="IAgent"/> performing
        /// it <seealso cref="IAgent.OnClose()"/> logic.
        /// <para>
        /// The clean up logic will only be performed once even if close is called from multiple concurrent threads.
        /// </para>
        /// </summary>
        public void Dispose()
        {
            _running = false;

            var thread = _thread.GetAndSet(Tombstone);
            if (Tombstone != thread)
            {
                if (null != thread)
                {
                    while (true)
                    {
                        try
                        {
                            thread.Join(1000);

                            if (!thread.IsAlive)
                            {
                                break;
                            }

                            Console.Error.WriteLine("timeout await for agent. Retrying...");

                            thread.Interrupt();
                        }
                        catch (ThreadInterruptedException)
                        {
                            return;
                        }
                    }
                }

                _agent.OnClose();
            }
        }
    }
}