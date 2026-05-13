/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
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
using System.Collections.Generic;
using System.Text;

namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// Group several <see cref="IAgent"/> into one composite so they can be scheduled as a unit.
    /// </summary>
    public class CompositeAgent : IAgent
    {
        private readonly IAgent[] _agents;
        private readonly string _roleName;
        private int _workIndex = 0;

        /// <param name="agents"> the parts of this composite, at least one agent and no null agents allowed</param>
        /// <exception cref="ArgumentException"> if an empty array of agents is provided</exception>
        /// <exception cref="NullReferenceException"> if the array or any element is null</exception>
        public CompositeAgent(List<IAgent> agents)
            : this(agents.ToArray())
        {
        }

        /// <param name="agents"> the parts of this composite, at least one agent and no null agents allowed</param>
        /// <exception cref="ArgumentException"> if an empty array of agents is provided</exception>
        /// <exception cref="NullReferenceException"> if the array or any element is null</exception>
        public CompositeAgent(params IAgent[] agents)
        {
            if (agents.Length == 0)
            {
                throw new ArgumentException("CompsiteAgent requires at least one sub-agent");
            }

            _agents = agents;

            var sb = new StringBuilder(agents.Length * 16);
            sb.Append('[');

            foreach (var agent in agents)
            {
                Objects.RequireNonNull(agent, "Agent cannot be null");
                sb.Append(agent.RoleName()).Append(',');
            }
            sb[sb.Length - 1] = ']';

            _roleName = sb.ToString();
        }

        /// <inheritdoc />
        public string RoleName()
        {
            return _roleName;
        }

        /// <inheritdoc /> Note that one agent throwing an exception on start may result in other agents not being
        /// started.
        ///
        /// <exception cref="Exception"> if any sub-agent throws an exception onClose. The first agent exception is
        /// collected as the inner exception of the thrown exception. </exception>
        public void OnStart()
        {
            Exception ce = null;
            foreach (IAgent agent in _agents)
            {
                try
                {
                    agent.OnStart();
                }
                catch (Exception ex)
                {
                    if (ce == null)
                    {
                        ce = new Exception("CompositeAgent: underlying agent error on start", ex);
                    }
                }
            }

            if (ce != null)
            {
                throw ce;
            }
        }

        /// <inheritdoc />
        public int DoWork()
        {
            int workCount = 0;

            IAgent[] agents = _agents;

            while (_workIndex < agents.Length)
            {
                var agent = agents[_workIndex++];
                workCount += agent.DoWork();
            }

            _workIndex = 0;

            return workCount;
        }

        /// <inheritdoc /> Note that one agent throwing an exception on close will not prevent other agents from being
        /// closed.
        ///
        /// <exception cref="Exception"> if any sub-agent throws an exception onClose. The first agent exception is
        /// collected as the inner exception of the thrown exception. </exception>
        public void OnClose()
        {
            Exception ce = null;
            foreach (IAgent agent in _agents)
            {
                try
                {
                    agent.OnClose();
                }
                catch (Exception ex)
                {
                    if (ce == null)
                    {
                        ce = new Exception("CompositeAgent: underlying agent error on close", ex);
                    }
                }
            }

            if (ce != null)
            {
                throw ce;
            }
        }
    }
}
