using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    internal class ServiceControlPublisher : IDisposable
    {
        private const int SEND_ATTEMPTS = 3;

        private readonly BufferClaim _bufferClaim = new BufferClaim();
        private readonly MessageHeaderEncoder _messageHeaderEncoder = new MessageHeaderEncoder();
        private readonly ScheduleTimerEncoder _scheduleTimerEncoder = new ScheduleTimerEncoder();
        private readonly CancelTimerEncoder _cancelTimerEncoder = new CancelTimerEncoder();
        private readonly ClusterActionAckEncoder _clusterActionAckEncoder = new ClusterActionAckEncoder();
        private readonly JoinLogEncoder _joinLogEncoder = new JoinLogEncoder();
        private readonly CloseSessionEncoder _closeSessionEncoder = new CloseSessionEncoder();
        private readonly Publication _publication;

        internal ServiceControlPublisher(Publication publication)
        {
            _publication = publication;
        }

        public void Dispose()
        {
            _publication?.Dispose();
        }

        public void ScheduleTimer(long correlationId, long deadlineMs)
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

                    return;
                }

                CheckResult(result);
            } while (--attempts > 0);

            throw new InvalidOperationException("Failed to schedule timer");
        }

        public void CancelTimer(long correlationId)
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

                    return;
                }

                CheckResult(result);
            } while (--attempts > 0);

            throw new InvalidOperationException("Failed to schedule timer");
        }
        
        public void AckAction(long logPosition, long leadershipTermId, int serviceId, ClusterAction action)
        {
            int length = MessageHeaderEncoder.ENCODED_LENGTH + ClusterActionAckEncoder.BLOCK_LENGTH;

            int attempts = SEND_ATTEMPTS;
            do
            {
                long result = _publication.TryClaim(length, _bufferClaim);
                if (result > 0)
                {
                    _clusterActionAckEncoder
                        .WrapAndApplyHeader(_bufferClaim.Buffer, _bufferClaim.Offset, _messageHeaderEncoder)
                        .LogPosition(logPosition)
                        .LeadershipTermId(leadershipTermId)
                        .ServiceId(serviceId)
                        .Action(action);

                    _bufferClaim.Commit();

                    return;
                }

                CheckResult(result);
            } while (--attempts > 0);

            throw new InvalidOperationException("Failed to send ACK");
        }

        public void JoinLog(long leadershipTermId, int commitPositionId, int logSessionId, int logStreamId, string channel)
        {
            int length = MessageHeaderEncoder.ENCODED_LENGTH + JoinLogEncoder.BLOCK_LENGTH + JoinLogEncoder.LogChannelHeaderLength() + channel.Length;

            int attempts = SEND_ATTEMPTS * 2;
            do
            {
                long result = _publication.TryClaim(length, _bufferClaim);
                if (result > 0)
                {
                    _joinLogEncoder
                        .WrapAndApplyHeader(_bufferClaim.Buffer, _bufferClaim.Offset, _messageHeaderEncoder)
                        .LeadershipTermId(leadershipTermId)
                        .CommitPositionId(commitPositionId)
                        .LogSessionId(logSessionId)
                        .LogStreamId(logStreamId)
                        .LogChannel(channel);

                    _bufferClaim.Commit();

                    return;
                }

                CheckResult(result);
            } while (--attempts > 0);

            throw new InvalidOperationException("Failed to send log connect request");
        }

        public void CloseSession(long clusterSessionId)
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

                    return;
                }

                CheckResult(result);
            } while (--attempts > 0);

            throw new InvalidOperationException("Failed to schedule timer");
        }

        private static void CheckResult(long result)
        {
            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new InvalidOperationException("Unexpected publication state: " + result);
            }
        }
    }
}