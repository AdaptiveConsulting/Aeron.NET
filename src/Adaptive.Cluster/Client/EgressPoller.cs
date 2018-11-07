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
        private readonly ChallengeDecoder challengeDecoder = new ChallengeDecoder();
        private readonly NewLeaderEventDecoder newLeaderEventDecoder = new NewLeaderEventDecoder();
        private readonly EgressMessageHeaderDecoder egressMessageHeaderDecoder = new EgressMessageHeaderDecoder();
        private readonly ControlledFragmentAssembler fragmentAssembler;
        private readonly Subscription subscription;
        private long clusterSessionId = Aeron.Aeron.NULL_VALUE;
        private long correlationId = Aeron.Aeron.NULL_VALUE;
        private long leadershipTermId = Aeron.Aeron.NULL_VALUE;
        private int leaderMemberId = Aeron.Aeron.NULL_VALUE;
        private int templateId = Aeron.Aeron.NULL_VALUE;
        private bool pollComplete;
        private EventCode eventCode;
        private string detail = "";
        private byte[] encodedChallenge;

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
        /// Cluster session id of the last polled event or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing.
        /// </summary>
        /// <returns> cluster session id of the last polled event or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if not returned. </returns>
        public long ClusterSessionId()
        {
            return clusterSessionId;
        }

        /// <summary>
        /// Correlation id of the last polled event or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing.
        /// </summary>
        /// <returns> correlation id of the last polled event or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if not returned. </returns>
        public long CorrelationId()
        {
            return correlationId;
        }
        
        /// <summary>
        /// Leadership term id of the last polled event or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing.
        /// </summary>
        /// <returns> leadership term id of the last polled event or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if not returned. </returns>
        public long LeadershipTermId()
        {
            return leadershipTermId;
        }
        
        /// <summary>
        /// Leader cluster member id of the last polled event or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing.
        /// </summary>
        /// <returns> leader cluster member id of the last polled event or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing. </returns>
        public int LeaderMemberId()
        {
            return leaderMemberId;
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
        public byte[] EncodedChallenge()
        {
            return encodedChallenge;
        }
        
        /// <summary>
        /// Has the last polling action received a complete event?
        /// </summary>
        /// <returns> true if the last polling action received a complete event. </returns>
        public bool IsPollComplete()
        {
            return pollComplete;
        }

        /// <summary>
        /// Was last message a challenge or not?
        /// </summary>
        /// <returns> true if last message was a challenge or false if not. </returns>
        public bool IsChallenged()
        {
            return ChallengeDecoder.TEMPLATE_ID == templateId;
        }

        public int Poll()
        {
            clusterSessionId = Aeron.Aeron.NULL_VALUE;
            correlationId = Aeron.Aeron.NULL_VALUE;
            leadershipTermId = Aeron.Aeron.NULL_VALUE;
            leaderMemberId = Aeron.Aeron.NULL_VALUE;
            templateId = Aeron.Aeron.NULL_VALUE;
            eventCode = Codecs.EventCode.NULL_VALUE;
            detail = "";
            encodedChallenge = null;
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
                    leadershipTermId = sessionEventDecoder.LeadershipTermId();
                    leaderMemberId = sessionEventDecoder.LeaderMemberId();
                    eventCode = sessionEventDecoder.Code();
                    detail = sessionEventDecoder.Detail();
                    break;

                case NewLeaderEventDecoder.TEMPLATE_ID:
                    newLeaderEventDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    clusterSessionId = newLeaderEventDecoder.ClusterSessionId();
                    leadershipTermId = newLeaderEventDecoder.LeadershipTermId();
                    leaderMemberId = newLeaderEventDecoder.LeaderMemberId();
                    detail = newLeaderEventDecoder.MemberEndpoints();
                    break;

                case EgressMessageHeaderDecoder.TEMPLATE_ID:
                    egressMessageHeaderDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    leadershipTermId = EgressMessageHeaderDecoder.LeadershipTermIdId();
                    clusterSessionId = egressMessageHeaderDecoder.ClusterSessionId();
                    break;

                case ChallengeDecoder.TEMPLATE_ID:
                    challengeDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    encodedChallenge = new byte[challengeDecoder.EncodedChallengeLength()];
                    challengeDecoder.GetEncodedChallenge(encodedChallenge, 0, challengeDecoder.EncodedChallengeLength());

                    clusterSessionId = challengeDecoder.ClusterSessionId();
                    correlationId = challengeDecoder.CorrelationId();
                    break;
                
                default:
                    throw new ClusterException("unknown templateId: " + templateId);
            }

            pollComplete = true;

            return ControlledFragmentHandlerAction.BREAK;
        }
    }
}