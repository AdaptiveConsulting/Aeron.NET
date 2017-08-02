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

        /// <param name="agents"> the parts of this composite, at least one agent and no null agents allowed</param>
        /// <exception cref="ArgumentException"> if an empty array of agents is provided</exception>
        /// <exception cref="NullReferenceException"> if the array or any element is null</exception>
        public CompositeAgent(List<IAgent> agents) : this(agents.ToArray())
        {
            
        }

        /// <param name="agents"> the parts of this composite, at least one agent and no null agents allowed</param>
        /// <exception cref="ArgumentException"> if an empty array of agents is provided</exception>
        /// <exception cref="NullReferenceException"> if the array or any element is null</exception>
        public CompositeAgent(params IAgent[] agents)
        {
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


        /// <inheritdoc />
        /// Note that one agent throwing an exception on start may result in other agents not being started.
        public void OnStart()
        {
            foreach (var agent in _agents)
            {
                agent.OnStart();
            }
        }

        /// <inheritdoc />
        public int DoWork()
        {
            int workCount = 0;

            foreach (var agent in _agents)
            {
                workCount += agent.DoWork();
            }

            return workCount;
        }

        /// <inheritdoc />
        /// Note that one agent throwing an exception on close may result in other agents not being closed.
        public void OnClose()
        {
            foreach (var agent in _agents)
            {
                agent.OnClose();
            }
        }
    }
}
