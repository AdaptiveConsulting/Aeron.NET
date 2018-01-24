using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona.Concurrent;
using Io.Aeron.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    internal class ConsensusModuleProxy : IDisposable
    {
        private const int SEND_ATTEMPTS = 3;

        private readonly long _serviceId;
        private readonly BufferClaim _bufferClaim = new BufferClaim();
        private readonly MessageHeaderEncoder _messageHeaderEncoder = new MessageHeaderEncoder();
        private readonly ScheduleTimerRequestEncoder _scheduleTimerRequestEncoder = new ScheduleTimerRequestEncoder();
        private readonly CancelTimerRequestEncoder _cancelTimerRequestEncoder = new CancelTimerRequestEncoder();
        private readonly ServiceActionAckEncoder _serviceActionAckEncoder = new ServiceActionAckEncoder();
        private readonly Publication _publication;
        private readonly IIdleStrategy _idleStrategy;

        internal ConsensusModuleProxy(long serviceId, Publication publication, IIdleStrategy idleStrategy)
        {
            _serviceId = serviceId;
            _publication = publication;
            _idleStrategy = idleStrategy;
        }

        public void Dispose()
        {
            _publication?.Dispose();
        }

        public void SendAcknowledgment(ServiceAction action, long logPosition, long leadershipTermId, long timestamp)
        {
            int length = MessageHeaderEncoder.ENCODED_LENGTH + ServiceActionAckEncoder.BLOCK_LENGTH;

            _idleStrategy.Reset();
            int attempts = SEND_ATTEMPTS;
            do
            {
                long result = _publication.TryClaim(length, _bufferClaim);
                if (result > 0)
                {
                    _serviceActionAckEncoder.WrapAndApplyHeader(_bufferClaim.Buffer, _bufferClaim.Offset, _messageHeaderEncoder)
                        .ServiceId(_serviceId)
                        .LogPosition(logPosition)
                        .LeadershipTermId(leadershipTermId)
                        .Timestamp(timestamp)
                        .Action(action);

                    _bufferClaim.Commit();

                    return;
                }

                CheckResult(result);
                _idleStrategy.Idle();
            } while (--attempts > 0);

            throw new InvalidOperationException("Failed to send ACK");
        }

        public void ScheduleTimer(long correlationId, long deadlineMs)
        {
            int length = MessageHeaderEncoder.ENCODED_LENGTH + ScheduleTimerRequestEncoder.BLOCK_LENGTH;

            _idleStrategy.Reset();
            int attempts = SEND_ATTEMPTS;
            do
            {
                long result = _publication.TryClaim(length, _bufferClaim);
                if (result > 0)
                {
                    _scheduleTimerRequestEncoder.WrapAndApplyHeader(_bufferClaim.Buffer, _bufferClaim.Offset, _messageHeaderEncoder)
                        .ServiceId(_serviceId)
                        .CorrelationId(correlationId)
                        .Deadline(deadlineMs);

                    _bufferClaim.Commit();

                    return;
                }

                CheckResult(result);
                _idleStrategy.Idle();
            } while (--attempts > 0);

            throw new InvalidOperationException("Failed to schedule timer");
        }

        public void CancelTimer(long correlationId)
        {
            int length = MessageHeaderEncoder.ENCODED_LENGTH + CancelTimerRequestEncoder.BLOCK_LENGTH;

            _idleStrategy.Reset();
            int attempts = SEND_ATTEMPTS;
            do
            {
                long result = _publication.TryClaim(length, _bufferClaim);
                if (result > 0)
                {
                    _cancelTimerRequestEncoder.WrapAndApplyHeader(_bufferClaim.Buffer, _bufferClaim.Offset, _messageHeaderEncoder)
                        .ServiceId(_serviceId)
                        .CorrelationId(correlationId);

                    _bufferClaim.Commit();

                    return;
                }

                CheckResult(result);
                _idleStrategy.Idle();
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