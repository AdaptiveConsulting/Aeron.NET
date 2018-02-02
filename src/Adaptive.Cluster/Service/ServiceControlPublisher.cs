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
        private readonly ScheduleTimerRequestEncoder _scheduleTimerRequestEncoder = new ScheduleTimerRequestEncoder();
        private readonly CancelTimerRequestEncoder _cancelTimerRequestEncoder = new CancelTimerRequestEncoder();
        private readonly ServiceActionAckEncoder _serviceActionAckEncoder = new ServiceActionAckEncoder();
        private readonly JoinLogRequestEncoder _joinLogRequestEncoder = new JoinLogRequestEncoder();
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
            int length = MessageHeaderEncoder.ENCODED_LENGTH + ScheduleTimerRequestEncoder.BLOCK_LENGTH;

            int attempts = SEND_ATTEMPTS;
            do
            {
                long result = _publication.TryClaim(length, _bufferClaim);
                if (result > 0)
                {
                    _scheduleTimerRequestEncoder
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
            int length = MessageHeaderEncoder.ENCODED_LENGTH + CancelTimerRequestEncoder.BLOCK_LENGTH;

            int attempts = SEND_ATTEMPTS;
            do
            {
                long result = _publication.TryClaim(length, _bufferClaim);
                if (result > 0)
                {
                    _cancelTimerRequestEncoder
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
            int length = MessageHeaderEncoder.ENCODED_LENGTH + ServiceActionAckEncoder.BLOCK_LENGTH;

            int attempts = SEND_ATTEMPTS;
            do
            {
                long result = _publication.TryClaim(length, _bufferClaim);
                if (result > 0)
                {
                    _serviceActionAckEncoder
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
            int length = MessageHeaderEncoder.ENCODED_LENGTH + JoinLogRequestEncoder.BLOCK_LENGTH + JoinLogRequestEncoder.LogChannelHeaderLength() + channel.Length;

            int attempts = SEND_ATTEMPTS * 2;
            do
            {
                long result = _publication.TryClaim(length, _bufferClaim);
                if (result > 0)
                {
                    _joinLogRequestEncoder
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

        private static void CheckResult(long result)
        {
            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new InvalidOperationException("Unexpected publication state: " + result);
            }
        }
    }
}