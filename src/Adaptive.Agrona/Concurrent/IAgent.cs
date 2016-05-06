using System;

namespace Adaptive.Agrona.Concurrent
{
    public interface IAgent
    {
        /// <summary>
        /// An agent should implement this method to do its work.
        /// 
        /// The return value is used for implementing a backoff strategy that can be employed when no work is
        /// currently available for the agent to process.
        /// </summary>
        /// <exception cref="Exception"> if an error has occurred </exception>
        /// <returns> 0 to indicate no work was currently available, a positive value otherwise. </returns>
        int DoWork();

        /// <summary>
        /// To be overridden by Agents that need to do resource cleanup on close.
        /// 
        /// This method will be called after the agent thread has terminated. It will only be called once by a single thread.
        /// 
        /// <b>Note:</b> Implementations of this method much be idempotent.
        /// </summary>
        void OnClose(); // default to do nothing unless you want to handle the notification.

        /// <summary>
        /// Get the name of this agent's role.
        /// </summary>
        /// <returns> the name of this agent's role. </returns>
        string RoleName();
    }
}