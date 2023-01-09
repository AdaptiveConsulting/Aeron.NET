namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Data class for holding the properties used when interacting with a cluster for local admin control.
    /// </summary>
    /// <seealso cref="ClusterMarkFile"></seealso>
    public class ClusterNodeControlProperties
    {
        /// <summary>
        /// Member id of the cluster to which the properties belong.
        /// </summary>
        public readonly int memberId;

        /// <summary>
        /// Stream id in the control channel on which the services listen.
        /// </summary>
        public readonly int serviceStreamId;

        /// <summary>
        /// Stream id in the control channel on which the consensus module listens.
        /// </summary>
        public readonly int consensusModuleStreamId;

        /// <summary>
        /// Directory where the Aeron Media Driver is running.
        /// </summary>
        public readonly string aeronDirectoryName;

        /// <summary>
        /// URI for the control channel.
        /// </summary>
        public readonly string controlChannel;

        /// <summary>
        /// Construct the set of properties for interacting with a cluster.
        /// </summary>
        /// <param name="memberId">                of the cluster to which the properties belong. </param>
        /// <param name="serviceStreamId">         in the control channel on which the services listen. </param>
        /// <param name="consensusModuleStreamId"> in the control channel on which the consensus module listens. </param>
        /// <param name="aeronDirectoryName">      where the Aeron Media Driver is running. </param>
        /// <param name="controlChannel">          for the services and consensus module. </param>
        public ClusterNodeControlProperties(
            int memberId,
            int serviceStreamId,
            int consensusModuleStreamId,
            string aeronDirectoryName,
            string controlChannel)
        {
            this.memberId = memberId;
            this.serviceStreamId = serviceStreamId;
            this.consensusModuleStreamId = consensusModuleStreamId;
            this.aeronDirectoryName = aeronDirectoryName;
            this.controlChannel = controlChannel;
        }
    }
}