namespace Adaptive.Agrona.Concurrent
{

    /// <summary>
    /// Idle strategy for use by threads when they do not have work to do.
    /// 
    /// <h3>Note regarding implementor state</h3>
    /// 
    /// Some implementations are known to be stateful, please note that you cannot safely assume implementations to be stateless.
    /// Where implementations are stateful it is recommended that implementation state is padded to avoid false sharing.
    /// 
    /// <h3>Note regarding potential for TTSP(Time To Safe Point) issues</h3>
    /// 
    /// If the caller spins in a 'counted' loop, and the implementation does not include a a safepoint poll this may cause a no
    /// (Time To SafePoint) problem. If this is the case for your application you can solve it by preventing the idle method from
    /// being inlined by setting the compilation symbol IDLESTRATEGY_NOINLINE.
    /// 
    /// </summary>
    public interface IIdleStrategy
	{
        /// <summary>
        /// Perform current idle action (e.g. nothing/yield/sleep). This method signature expects users to call into it on every work
        /// 'cycle'. The implementations may use the indication "workCount &gt; 0" to reset internal backoff state. This method works
        /// well with 'work' APIs which follow the following rules:
        /// <ul>
        /// <li>'work' returns a value larger than 0 when some work has been done</li>
        /// <li>'work' returns 0 when no work has been done</li>
        /// <li>'work' may return error codes which are less than 0, but which amount to no work has been done</li>
        /// </ul>
        /// 
        /// Callers are expected to follow this pattern:
        /// 
        /// <pre>
        /// <code>
        /// while (isRunning)
        /// {
        ///     idleStrategy.Idle(doWork());
        /// }
        /// </code>
        /// </pre>
        /// 
        /// </summary>
        /// <param name="workCount"> performed in last duty cycle. </param>
        void Idle(int workCount);

        /// <summary>
        /// Perform current idle action (e.g. nothing/yield/sleep). To be used in conjunction with <seealso cref="Reset()"/>
        /// to clear internal state when idle period is over (or before it begins). Callers are expected to follow this pattern:
        /// 
        /// <pre>
        /// <code>
        /// while (isRunning)
        /// {
        ///   if (!hasWork())
        ///   {
        ///     idleStrategy.Reset();
        ///     while (!hasWork())
        ///     {
        ///       if (!isRunning)
        ///       {
        ///         return;
        ///       }
        ///       idleStrategy.Idle();
        ///     }
        ///   }
        ///   doWork();
        /// }
        /// </code>
        /// </pre>
        /// </summary>
        void Idle();

		/// <summary>
		/// Reset the internal state in preparation for entering an idle state again.
		/// </summary>
		void Reset();
	}

}