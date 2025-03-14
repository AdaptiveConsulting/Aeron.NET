using System;
using Adaptive.Aeron;
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Proxy for communicating with the Consensus Module over IPC.
    /// <para>
    /// Note: This class is not for public use.
    /// </para>
    /// </summary>
    public class ConsensusModuleProxy : IDisposable
    {
        private const int SEND_ATTEMPTS = 3;

        private readonly BufferClaim _bufferClaim = new BufferClaim();
        private readonly MessageHeaderEncoder _messageHeaderEncoder = new MessageHeaderEncoder();
        private readonly ScheduleTimerEncoder _scheduleTimerEncoder = new ScheduleTimerEncoder();
        private readonly CancelTimerEncoder _cancelTimerEncoder = new CancelTimerEncoder();
        private readonly ServiceAckEncoder _serviceAckEncoder = new ServiceAckEncoder();
        private readonly CloseSessionEncoder _closeSessionEncoder = new CloseSessionEncoder();
        private readonly ClusterMembersQueryEncoder _clusterMembersQueryEncoder = new ClusterMembersQueryEncoder();
        private readonly Publication _publication;

        /// <summary>
        /// Construct a proxy to the consensus module that will send messages over a provided <seealso cref="Publication"/>.
        /// </summary>
        /// <param name="publication"> for sending messages to the consensus module. </param>
        public ConsensusModuleProxy(Publication publication)
        {
            _publication = publication;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _publication?.Dispose();
        }

        public bool ScheduleTimer(long correlationId, long deadline)
        {
            int length = MessageHeaderEncoder.ENCODED_LENGTH + ScheduleTimerEncoder.BLOCK_LENGTH;

            int attempts = SEND_ATTEMPTS;
            do
            {
                long result = _publication.TryClaim(length, _bufferClaim);
                if (result > 0)
                {
                    _scheduleTimerEncoder
                        .WrapAndApplyHeader(_bufferClaim.Buffer, _bufferClaim.Offset, _messageHeaderEncoder)
                        .CorrelationId(correlationId)
                        .Deadline(deadline);

                    _bufferClaim.Commit();

                    return true;
                }

                CheckResult(result);
            } while (--attempts > 0);

            return false;
        }

        internal bool CancelTimer(long correlationId)
        {
            int length = MessageHeaderEncoder.ENCODED_LENGTH + CancelTimerEncoder.BLOCK_LENGTH;

            int attempts = SEND_ATTEMPTS;
            do
            {
                long result = _publication.TryClaim(length, _bufferClaim);
                if (result > 0)
                {
                    _cancelTimerEncoder
                        .WrapAndApplyHeader(_bufferClaim.Buffer, _bufferClaim.Offset, _messageHeaderEncoder)
                        .CorrelationId(correlationId);

                    _bufferClaim.Commit();

                    return true;
                }

                CheckResult(result);
            } while (--attempts > 0);

            return false;
        }
        
        internal long Offer(
            IDirectBuffer headerBuffer,
            int headerOffset,
            int headerLength,
            IDirectBuffer messageBuffer,
            int messageOffset,
            int messageLength)
        {
            return _publication.Offer(headerBuffer, headerOffset, headerLength, messageBuffer, messageOffset, messageLength);
        }

        internal long Offer(DirectBufferVector[] vectors)
        {
            return _publication.Offer(vectors, null);
        }

        internal long TryClaim(int length, BufferClaim bufferClaim, IDirectBuffer sessionHeader)
        {
            long result = _publication.TryClaim(length, bufferClaim);
            if (result > 0)
            {
                bufferClaim.PutBytes(sessionHeader, 0, AeronCluster.SESSION_HEADER_LENGTH);
            }

            return result;
        }

        internal bool Ack(long logPosition, long timestamp, long ackId, long relevantId, int serviceId)
        {
            int length = MessageHeaderEncoder.ENCODED_LENGTH + ServiceAckEncoder.BLOCK_LENGTH;

            int attempts = SEND_ATTEMPTS;
            do
            {
                long result = _publication.TryClaim(length, _bufferClaim);
                if (result > 0)
                {
                    _serviceAckEncoder
                        .WrapAndApplyHeader(_bufferClaim.Buffer, _bufferClaim.Offset, _messageHeaderEncoder)
                        .LogPosition(logPosition)
                        .Timestamp(timestamp)
                        .AckId(ackId)
                        .RelevantId(relevantId)
                        .ServiceId(serviceId);

                    _bufferClaim.Commit();

                    return true;
                }

                CheckResult(result);
            } while (--attempts > 0);

            return false;
        }

        internal bool CloseSession(long clusterSessionId)
        {
            int length = MessageHeaderEncoder.ENCODED_LENGTH + CloseSessionEncoder.BLOCK_LENGTH;

            int attempts = SEND_ATTEMPTS;
            do
            {
                long result = _publication.TryClaim(length, _bufferClaim);
                if (result > 0)
                {
                    _closeSessionEncoder
                        .WrapAndApplyHeader(_bufferClaim.Buffer, _bufferClaim.Offset, _messageHeaderEncoder)
                        .ClusterSessionId(clusterSessionId);

                    _bufferClaim.Commit();

                    return true;
                }

                CheckResult(result);
            } while (--attempts > 0);

            return false;
        }

        /// <summary>
        /// Query for the current cluster members.
        /// </summary>
        /// <param name="correlationId"> for the request. </param>
        /// <returns> true of the request was successfully sent, otherwise false. </returns>
        public bool ClusterMembersQuery(long correlationId)
        {
            int length = MessageHeaderEncoder.ENCODED_LENGTH + ClusterMembersQueryEncoder.BLOCK_LENGTH;

            int attempts = SEND_ATTEMPTS;
            do
            {
                long result = _publication.TryClaim(length, _bufferClaim);
                if (result > 0)
                {
                    _clusterMembersQueryEncoder
                        .WrapAndApplyHeader(_bufferClaim.Buffer, _bufferClaim.Offset, _messageHeaderEncoder)
                        .CorrelationId(correlationId)
                        .Extended(BooleanType.TRUE);

                    _bufferClaim.Commit();

                    return true;
                }

                CheckResult(result);
            } while (--attempts > 0);

            return false;
        }

        private static void CheckResult(long result)
        {
            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new ClusterException("unexpected publication state: " + result);
            }
        }
    }
}