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
    /// This class is not for public use.
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
        private readonly RemoveMemberEncoder _removeMemberEncoder = new RemoveMemberEncoder();
        private readonly Publication _publication;

        public ConsensusModuleProxy(Publication publication)
        {
            _publication = publication;
        }

        public void Dispose()
        {
            _publication?.Dispose();
        }

        public bool ScheduleTimer(long correlationId, long deadlineMs)
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
                        .Deadline(deadlineMs);

                    _bufferClaim.Commit();

                    return true;
                }

                CheckResult(result);
            } while (--attempts > 0);

            return false;
        }

        public bool CancelTimer(long correlationId)
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
        
        public long Offer(
            IDirectBuffer headerBuffer,
            int headerOffset,
            int headerLength,
            IDirectBuffer messageBuffer,
            int messageOffset,
            int messageLength)
        {
            return _publication.Offer(headerBuffer, headerOffset, headerLength, messageBuffer, messageOffset, messageLength);
        }

        public long Offer(DirectBufferVector[] vectors)
        {
            return _publication.Offer(vectors, null);
        }

        public long TryClaim(int length, BufferClaim bufferClaim, IDirectBuffer sessionHeader)
        {
            long result = _publication.TryClaim(length, bufferClaim);
            if (result > 0)
            {
                bufferClaim.PutBytes(sessionHeader, 0, AeronCluster.SESSION_HEADER_LENGTH);
            }

            return result;
        }

        public bool Ack(long logPosition, long timestamp, long ackId, long relevantId, int serviceId)
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

        public bool CloseSession(long clusterSessionId)
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

        public bool RemoveMember(int memberId, BooleanType isPassive)
        {
            int length = MessageHeaderEncoder.ENCODED_LENGTH + RemoveMemberEncoder.BLOCK_LENGTH;

            int attempts = SEND_ATTEMPTS;
            do
            {
                long result = _publication.TryClaim(length, _bufferClaim);
                if (result > 0)
                {
                    _removeMemberEncoder
                        .WrapAndApplyHeader(_bufferClaim.Buffer, _bufferClaim.Offset, _messageHeaderEncoder)
                        .MemberId(memberId)
                        .IsPassive(isPassive);

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
                throw new AeronException("unexpected publication state: " + result);
            }
        }
    }
}