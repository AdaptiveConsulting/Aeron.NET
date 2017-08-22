/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Threading;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// Agent runner containing an <see cref="IAgent"/> which is run on a <see cref="Thread"/>.
    /// <para>
    /// Note: An instance should only be started once and then discarded, it should not be reused.
    /// </para>
    /// </summary>
    public class AgentRunner : IDisposable
    {
        /// <summary>
        /// Indicates that the runner is being closed.
        /// </summary>
        private static readonly Thread TOMBSTONE = null;

        private static readonly int RETRY_CLOSE_TIMEOUT_MS = 3000;
        
        private volatile bool _isRunning = true;

        /// <summary>
        /// Has the <see cref="IAgent"/> been closed?
        /// </summary>
        public bool IsClosed { get; private set; }

        private readonly AtomicCounter _errorCounter;
        private readonly ErrorHandler _errorHandler;
        private readonly IIdleStrategy _idleStrategy;
        private readonly IAgent _agent;
        private readonly AtomicReference<Thread> _thread = new AtomicReference<Thread>();

        /// <summary>
        /// Create an agent runner and initialise it.
        /// </summary>
        /// <param name="idleStrategy"> to use for Agent run loop </param>
        /// <param name="errorHandler"> to be called if an <seealso cref="Exception"/> is encountered </param>
        /// <param name="errorCounter"> to be incremented each time an exception is encountered. This may be null.</param>
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
        /// <returns>  the new thread that has been started.</returns>
        public static Thread StartOnThread(AgentRunner runner)
        {
            var thread = new Thread(runner.Run)
            {
                Name = runner.Agent().RoleName()
            };
            thread.Start();
            return thread;
        }

        /// <summary>
        /// Start the given agent runner on a new thread.
        /// </summary>
        /// <param name="runner"> the agent runner to start </param>
        /// <param name="threadFactory"> the factory to use to create the thread.</param>
        /// <returns>  the new thread that has been started.</returns>
        public static Thread StartOnThread(AgentRunner runner, IThreadFactory threadFactory)
        {
            var thread = threadFactory.NewThread(runner.Run);
            thread.Name = runner.Agent().RoleName();
            thread.Start();
            return thread;
        }

        /// <summary>
        /// The <seealso cref="IAgent"/> which is contained.
        /// </summary>
        /// <returns> <seealso cref="IAgent"/> being contained.</returns>
        public IAgent Agent()
        {
            return _agent;
        }

        public Thread Thread()
        {
            return _thread.Get();
        }

        /// <summary>
        /// Run an <seealso cref="IAgent"/>.
        /// <para>
        /// This method does not return until the run loop is stopped via <seealso cref="Dispose()"/>.
        /// </para>
        /// </summary>
        public void Run()
        {
            try
            {
                if (!_thread.CompareAndSet(null, System.Threading.Thread.CurrentThread))
                {
                    return;
                }

                var idleStrategy = _idleStrategy;
                var agent = _agent;

                try
                {
                    agent.OnStart();
                }
                catch (Exception ex)
                {
                    HandleError(ex);
                    _isRunning = false;
                }

                while (_isRunning)
                {
                    if (DoDutyCycle(idleStrategy, agent))
                    {
                        break;
                    }
                }

                try
                {
                    agent.OnClose();
                }
                catch (Exception ex)
                {
                    HandleError(ex);
                }
            }
            finally
            {
                IsClosed = true;
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
            _isRunning = false;

            var thread = _thread.GetAndSet(TOMBSTONE);
            if (TOMBSTONE != thread && null != thread)
            {
                while (true)
                {
                    try
                    {
                        thread.Join(RETRY_CLOSE_TIMEOUT_MS);

                        if (!thread.IsAlive || IsClosed)
                        {
                            return;
                        }

                        Console.Error.WriteLine($"Timeout waiting for agent '{_agent.RoleName()}' to close, Retrying...");

                        thread.Interrupt();
                    }
                    catch (ThreadInterruptedException)
                    {
                        return;
                    }
                }
            }
        }

        private bool DoDutyCycle(IIdleStrategy idleStrategy, IAgent agent)
        {
            try
            {
                idleStrategy.Idle(agent.DoWork());
            }
            catch (ThreadInterruptedException)
            {
                return true;
            }
            catch (AgentTerminationException ex)
            {
                HandleError(ex);
                return true;
            }
            catch (Exception ex)
            {
                HandleError(ex);
            }

            return false;
        }

        private void HandleError(Exception exception)
        {
            if (_isRunning)
            {
                _errorCounter?.Increment();
                _errorHandler(exception);
            }
        }
    }
}