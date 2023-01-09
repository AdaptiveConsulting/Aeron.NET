using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Codecs;
using static Adaptive.Aeron.LogBuffer.ControlledFragmentHandlerAction;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Poller for the egress from a cluster to capture administration message details.
    /// </summary>
    public class EgressPoller : IControlledFragmentHandler
    {
        private readonly int fragmentLimit;
        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly SessionEventDecoder sessionEventDecoder = new SessionEventDecoder();
        private readonly ChallengeDecoder challengeDecoder = new ChallengeDecoder();
        private readonly NewLeaderEventDecoder newLeaderEventDecoder = new NewLeaderEventDecoder();
        private readonly SessionMessageHeaderDecoder sessionMessageHeaderDecoder = new SessionMessageHeaderDecoder();
        private readonly ControlledFragmentAssembler fragmentAssembler;
        private readonly Subscription subscription;

        private Image egressImage;
        private long clusterSessionId = Aeron.Aeron.NULL_VALUE;
        private long correlationId = Aeron.Aeron.NULL_VALUE;
        private long leadershipTermId = Aeron.Aeron.NULL_VALUE;
        private int leaderMemberId = Aeron.Aeron.NULL_VALUE;
        private int templateId = Aeron.Aeron.NULL_VALUE;
        private int version = 0;
        private bool isPollComplete = false;
        private EventCode eventCode;
        private string detail = "";
        private byte[] encodedChallenge;

        /// <summary>
        /// Construct a poller on the egress subscription.
        /// </summary>
        /// <param name="subscription">  for egress from the cluster. </param>
        /// <param name="fragmentLimit"> for each poll operation. </param>
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
        /// <seealso cref="Image"/> for the egress response from the cluster which can be used for connection tracking.
        /// </summary>
        /// <returns> <seealso cref="Image"/> for the egress response from the cluster which can be used for connection tracking. </returns>
        public Image EgressImage()
        {
            return egressImage;
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
        /// Version response from the server in semantic version form.
        /// </summary>
        /// <returns> response from the server in semantic version form. </returns>
        public int Version()
        {
            return version;
        }

        /// <summary>
        /// Get the detail returned from the last session event.
        /// </summary>
        /// <returns> the detail returned from the last session event. </returns>
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
            return isPollComplete;
        }

        /// <summary>
        /// Was last message a challenge or not?
        /// </summary>
        /// <returns> true if last message was a challenge or false if not. </returns>
        public bool IsChallenged()
        {
            return ChallengeDecoder.TEMPLATE_ID == templateId;
        }

        /// <summary>
        /// Reset last captured value and poll the egress subscription for output.
        /// </summary>
        /// <returns> number of fragments consumed. </returns>
        public int Poll()
        {
            if (isPollComplete)
            {
                clusterSessionId = Aeron.Aeron.NULL_VALUE;
                correlationId = Aeron.Aeron.NULL_VALUE;
                leadershipTermId = Aeron.Aeron.NULL_VALUE;
                leaderMemberId = Aeron.Aeron.NULL_VALUE;
                templateId = Aeron.Aeron.NULL_VALUE;
                version = 0;
                eventCode = Codecs.EventCode.NULL_VALUE;
                detail = "";
                encodedChallenge = null;
                isPollComplete = false;
            }

            return subscription.ControlledPoll(fragmentAssembler, fragmentLimit);
        }

        /// <inheritdoc />
        public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            if (isPollComplete)
            {
                return ABORT;
            }

            messageHeaderDecoder.Wrap(buffer, offset);

            int schemaId = messageHeaderDecoder.SchemaId();
            if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
            {
                throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" +
                                           schemaId);
            }

            templateId = messageHeaderDecoder.TemplateId();
            switch (templateId)
            {
                case SessionMessageHeaderDecoder.TEMPLATE_ID:
                    sessionMessageHeaderDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    leadershipTermId = sessionMessageHeaderDecoder.LeadershipTermId();
                    clusterSessionId = sessionMessageHeaderDecoder.ClusterSessionId();
                    isPollComplete = true;
                    return BREAK;

                case SessionEventDecoder.TEMPLATE_ID:
                    sessionEventDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    clusterSessionId = sessionEventDecoder.ClusterSessionId();
                    correlationId = sessionEventDecoder.CorrelationId();
                    leadershipTermId = sessionEventDecoder.LeadershipTermId();
                    leaderMemberId = sessionEventDecoder.LeaderMemberId();
                    eventCode = sessionEventDecoder.Code();
                    version = sessionEventDecoder.Version();
                    detail = sessionEventDecoder.Detail();
                    isPollComplete = true;
                    egressImage = (Image)header.Context;
                    return BREAK;

                case NewLeaderEventDecoder.TEMPLATE_ID:
                    newLeaderEventDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    clusterSessionId = newLeaderEventDecoder.ClusterSessionId();
                    leadershipTermId = newLeaderEventDecoder.LeadershipTermId();
                    leaderMemberId = newLeaderEventDecoder.LeaderMemberId();
                    detail = newLeaderEventDecoder.IngressEndpoints();
                    isPollComplete = true;
                    egressImage = (Image)header.Context;
                    return BREAK;

                case ChallengeDecoder.TEMPLATE_ID:
                    challengeDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    encodedChallenge = new byte[challengeDecoder.EncodedChallengeLength()];
                    challengeDecoder.GetEncodedChallenge(encodedChallenge, 0,
                        challengeDecoder.EncodedChallengeLength());

                    clusterSessionId = challengeDecoder.ClusterSessionId();
                    correlationId = challengeDecoder.CorrelationId();
                    isPollComplete = true;
                    return BREAK;
            }

            return CONTINUE;
        }
    }
}