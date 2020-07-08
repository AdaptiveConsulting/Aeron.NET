using System.Collections.Generic;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron.Status
{
    /// <summary>
    /// Counter used to store the status of a bind address and port for the local end of a channel.
    /// <para>
    /// When the value is <seealso cref="ChannelEndpointStatus.ACTIVE"/> then the key value and label will be updated with the
    /// socket address and port which is bound.
    /// </para>
    /// </summary>
    public class LocalSocketAddressStatus
    {
        private const int CHANNEL_STATUS_ID_OFFSET = 0;
        private static readonly int LOCAL_SOCKET_ADDRESS_LENGTH_OFFSET = CHANNEL_STATUS_ID_OFFSET + BitUtil.SIZE_OF_INT;

        private static readonly int LOCAL_SOCKET_ADDRESS_STRING_OFFSET =
            LOCAL_SOCKET_ADDRESS_LENGTH_OFFSET + BitUtil.SIZE_OF_INT;

        private static readonly int MAX_IPV6_LENGTH = "[ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255]:65536".Length;

        /// <summary>
        /// Initial length length for a key, this will be expanded later when bound.
        /// </summary>
        public static readonly int INITIAL_LENGTH = BitUtil.SIZE_OF_INT * 2;

        /// <summary>
        /// Type of the counter used to track a local socket address and port.
        /// </summary>
        public const int LOCAL_SOCKET_ADDRESS_STATUS_TYPE_ID = 14;

        /// <summary>
        /// Find the list of currently bound local sockets.
        /// </summary>
        /// <param name="countersReader">  for the connected driver. </param>
        /// <param name="channelStatus">   value for the channel which aggregates the transports. </param>
        /// <param name="channelStatusId"> identity of the counter for the channel which aggregates the transports. </param>
        /// <returns> the list of active bound local socket addresses. </returns>
        public static List<string> FindAddresses(CountersReader countersReader, long channelStatus, int channelStatusId)
        {
            if (channelStatus != ChannelEndpointStatus.ACTIVE)
            {
                return new List<string>();
            }

            List<string> bindings = new List<string>(2);
            IDirectBuffer buffer = countersReader.MetaDataBuffer;

            for (int i = 0, size = countersReader.MaxCounterId; i < size; i++)
            {
                if (countersReader.GetCounterState(i) == CountersReader.RECORD_ALLOCATED &&
                    countersReader.GetCounterTypeId(i) == LOCAL_SOCKET_ADDRESS_STATUS_TYPE_ID)
                {
                    int recordOffset = CountersReader.MetaDataOffset(i);
                    int keyIndex = recordOffset + CountersReader.KEY_OFFSET;

                    if (channelStatusId == buffer.GetInt(keyIndex + CHANNEL_STATUS_ID_OFFSET) &&
                        ChannelEndpointStatus.ACTIVE == countersReader.GetCounterValue(i))
                    {
                        int length = buffer.GetInt(keyIndex + LOCAL_SOCKET_ADDRESS_LENGTH_OFFSET);
                        if (length > 0)
                        {
                            bindings.Add(buffer.GetStringWithoutLengthAscii(
                                keyIndex + LOCAL_SOCKET_ADDRESS_STRING_OFFSET,
                                length));
                        }
                    }
                }
            }

            return bindings;
        }
    }
}