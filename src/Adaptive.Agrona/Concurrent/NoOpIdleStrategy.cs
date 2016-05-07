namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// Low-latency idle strategy to be employed in loops that do significant work on each iteration such that any work in the
    /// idle strategy would be wasteful.
    /// </summary>
    public sealed class NoOpIdleStrategy : IIdleStrategy
    {
        /// <summary>
        /// <b>Note</b>: this implementation will result in no safepoint poll once inlined.
        /// </summary>
        /// <seealso cref="IIdleStrategy" />
        public void Idle(int workCount)
        {
        }

        /// <summary>
        /// <b>Note</b>: this implementation will result in no safepoint poll once inlined.
        /// </summary>
        /// <seealso cref="IIdleStrategy" />
        public void Idle()
        {
        }

        public void Reset()
        {
        }
    }
}