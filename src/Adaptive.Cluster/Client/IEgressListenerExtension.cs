using Adaptive.Agrona;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Interface for consuming messages coming from the cluster for an unknown schema.
    /// </summary>
    public interface IEgressListenerExtension
    {
        /// <summary>
        /// Message of unknown schema to egress that can be handled by specific listener implementation.
        /// </summary>
        /// <param name="actingBlockLength"> acting block length from header. </param>
        /// <param name="templateId">        template id. </param>
        /// <param name="schemaId">          schema id. </param>
        /// <param name="actingVersion">     acting version. </param>
        /// <param name="buffer">            message buffer. </param>
        /// <param name="offset">            message offset. </param>
        /// <param name="length">            message length. </param>
        void OnExtensionMessage(
            int actingBlockLength,
            int templateId,
            int schemaId,
            int actingVersion,
            IDirectBuffer buffer,
            int offset,
            int length);
    }
}