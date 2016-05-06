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
        private static readonly Thread TOMBSTONE = new Thread();

        private volatile bool Running = true;

        private readonly AtomicCounter ErrorCounter;
        private readonly ErrorHandler ErrorHandler;
        private readonly IIdleStrategy IdleStrategy;
        private readonly IAgent Agent_Renamed;
        private readonly AtomicReference<Thread> Thread = new AtomicReference<Thread>();

        /// <summary>
        /// Create an agent passing in <seealso cref="IdleStrategy"/>
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


            IdleStrategy = idleStrategy;
            ErrorHandler = errorHandler;
            ErrorCounter = errorCounter;
            Agent_Renamed = agent;
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
            return Agent_Renamed;
        }

        /// <summary>
        /// Run an <seealso cref="IAgent"/>.
        /// <para>
        /// This method does not return until the run loop is stopped via <seealso cref="Dispose()"/>.
        /// </para>
        /// </summary>
        public void Run()
        {
            if (!Thread.compareAndSet(null, Thread.CurrentThread))
            {
                return;
            }

            var idleStrategy = IdleStrategy;
            var agent = Agent_Renamed;
            while (Running)
            {
                try
                {
                    idleStrategy.Idle(agent.DoWork());
                }
                catch (ThreadInterruptedException)
                {
                    Thread.interrupted();
                }
                catch (va.nio.channels.ClosedByInterruptException)
                {
                    // Deliberately blank, if this exception is thrown then your interrupted status will be set.
                }
                catch (Exception ex)
                {
                    if (null != ErrorCounter)
                    {
                        ErrorCounter.Increment();
                    }

                    this.ErrorHandler(ex);
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
            Running = false;

            Thread thread = Thread.getAndSet(TOMBSTONE);
            if (TOMBSTONE != thread)
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
                            Thread.CurrentThread.Interrupt();
                            return;
                        }
                    }
                }

                Agent_Renamed.OnClose();
            }
        }
    }
}