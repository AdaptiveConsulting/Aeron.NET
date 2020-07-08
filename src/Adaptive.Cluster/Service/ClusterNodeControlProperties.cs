namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Data class for holding the properties used when interacting with a cluster for local admin control.
    /// </summary>
    /// <seealso cref="ClusterMarkFile"></seealso>
    public class ClusterNodeControlProperties
    {
        public readonly string aeronDirectoryName;
        public readonly string controlChannel;
        public readonly int serviceStreamId;
        public readonly int consensusModuleStreamId;

        public ClusterNodeControlProperties(int serviceStreamId, int consensusModuleStreamId, string aeronDirectoryName,
            string controlChannel)
        {
            this.aeronDirectoryName = aeronDirectoryName;
            this.controlChannel = controlChannel;
            this.serviceStreamId = serviceStreamId;
            this.consensusModuleStreamId = consensusModuleStreamId;
        }
    }
}