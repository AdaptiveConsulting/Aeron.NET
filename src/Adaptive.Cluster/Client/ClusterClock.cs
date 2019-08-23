using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// A clock representing a number of <seealso cref="ClusterTimeUnit"/>s since 1 Jan 1970 UTC. Defaults to <seealso cref="ClusterTimeUnit.MILLIS"/>.
    /// <para>
    /// This is the clock used to timestamp sequenced messages for the cluster log and timers. Implementations should
    /// be efficient otherwise throughput of the cluster will be impacted due to frequency of use.
    /// </para>
    /// </summary>
    public abstract class ClusterClock
    {
        /// <summary>
        /// The unit of time returned from the <seealso cref="Time()"/> method.
        /// </summary>
        /// <returns> the unit of time returned from the <seealso cref="Time()"/> method. </returns>
        public virtual ClusterTimeUnit TimeUnit()
        {
            return ClusterTimeUnit.MILLIS;
        }

        /// <summary>
        /// The count of <seealso cref="TimeUnit()"/>s since 1 Jan 1970 UTC.
        /// </summary>
        /// <returns> the count of <seealso cref="TimeUnit()"/>s since 1 Jan 1970 UTC. </returns>
        public abstract long Time();
    }
}