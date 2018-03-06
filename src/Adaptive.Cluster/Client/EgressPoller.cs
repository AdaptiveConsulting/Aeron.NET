using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Client
{
    public class EgressPoller : IControlledFragmentHandler
    {
        private readonly int fragmentLimit;
        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly SessionEventDecoder sessionEventDecoder = new SessionEventDecoder();
        private readonly NewLeaderEventDecoder newLeaderEventDecoder = new NewLeaderEventDecoder();
        private readonly SessionHeaderDecoder sessionHeaderDecoder = new SessionHeaderDecoder();
        private readonly ChallengeDecoder challengeDecoder = new ChallengeDecoder();
        private readonly ControlledFragmentAssembler fragmentAssembler;
        private readonly Subscription subscription;
        private long clusterSessionId = -1;
        private long correlationId = -1;
        private int templateId = -1;
        private bool pollComplete;
        private EventCode eventCode;
        private string detail = "";
        private byte[] challengeData;

        public EgressPoller(Subscription subscription, int fragmentLimit)
        {
            this.fragmentAssembler = new ControlledFragmentAssembler(this);

            this.subscription = subscription;
            this.fragmentLimit = fragmentLimit;
        }

        /// <summary>
        /// Get the <seealso cref="Subscription"/> used for polling events.
        /// </summary>
        /// <returns> the <seealso cref="Subscription"/> used for polling events. </returns>
        public Subscription Subscription()
        {
            return subscription;
        }

        /// <summary>
        /// Get the template id of the last received event.
        /// </summary>
        /// <returns> the template id of the last received event. </returns>
        public int TemplateId()
        {
            return templateId;
        }

        /// <summary>
        /// Cluster session id of the last polled event or -1 if poll returned nothing.
        /// </summary>
        /// <returns> cluster session id of the last polled event or -1 if unrecognised template. </returns>
        public long ClusterSessionId()
        {
            return clusterSessionId;
        }

        /// <summary>
        /// Correlation id of the last polled event or -1 if poll returned nothing.
        /// </summary>
        /// <returns> correlation id of the last polled event or -1 if unrecognised template. </returns>
        public long CorrelationId()
        {
            return correlationId;
        }

        /// <summary>
        /// Get the event code returned from the last session event.
        /// </summary>
        /// <returns> the event code returned from the last session event. </returns>
        public EventCode EventCode()
        {
            return eventCode;
        }

        /// <summary>
        /// Get the detail returned in the last session event.
        /// </summary>
        /// <returns> the detail returned in the last session event. </returns>
        public string Detail()
        {
            return detail;
        }

        /// <summary>
        /// Get the challenge data in the last challenge.
        /// </summary>
        /// <returns> the challenge data in the last challenge or null if last message was not a challenge. </returns>
        public byte[] ChallengeData()
        {
            return challengeData;
        }

        /// <summary>
        /// Has the last polling action received a complete event?
        /// </summary>
        /// <returns> true of the last polling action received a complete event? </returns>
        public bool IsPollComplete()
        {
            return pollComplete;
        }

        /// <summary>
        /// Was last message a challenge or not.
        /// </summary>
        /// <returns> true if last message was a challenge or false if not. </returns>
        public bool Challenged()
        {
            return ChallengeDecoder.TEMPLATE_ID == templateId;
        }

        public int Poll()
        {
            clusterSessionId = -1;
            correlationId = -1;
            templateId = -1;
            eventCode = Codecs.EventCode.NULL_VALUE;
            detail = "";
            challengeData = null;
            pollComplete = false;

            return subscription.ControlledPoll(fragmentAssembler, fragmentLimit);
        }

        public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            messageHeaderDecoder.Wrap(buffer, offset);

            templateId = messageHeaderDecoder.TemplateId();
            switch (templateId)
            {
                case SessionEventDecoder.TEMPLATE_ID:
                    sessionEventDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    clusterSessionId = sessionEventDecoder.ClusterSessionId();
                    correlationId = sessionEventDecoder.CorrelationId();
                    eventCode = sessionEventDecoder.Code();
                    detail = sessionEventDecoder.Detail();
                    break;

                case NewLeaderEventDecoder.TEMPLATE_ID:
                    newLeaderEventDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    clusterSessionId = newLeaderEventDecoder.ClusterSessionId();
                    break;

                case SessionHeaderDecoder.TEMPLATE_ID:
                    sessionHeaderDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    clusterSessionId = sessionHeaderDecoder.ClusterSessionId();
                    correlationId = sessionHeaderDecoder.CorrelationId();
                    break;

                case ChallengeDecoder.TEMPLATE_ID:
                    challengeDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    challengeData = new byte[challengeDecoder.ChallengeDataLength()];
                    challengeDecoder.GetChallengeData(challengeData, 0, challengeDecoder.ChallengeDataLength());

                    clusterSessionId = challengeDecoder.ClusterSessionId();
                    correlationId = challengeDecoder.CorrelationId();
                    break;

                default:
                    throw new InvalidOperationException("Unknown templateId: " + templateId);
            }

            pollComplete = true;

            return ControlledFragmentHandlerAction.BREAK;
        }
    }
}