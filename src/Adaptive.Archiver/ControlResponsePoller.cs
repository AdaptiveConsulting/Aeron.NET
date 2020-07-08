using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Archiver.Codecs;
using static Adaptive.Aeron.LogBuffer.ControlledFragmentHandlerAction;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Encapsulate the polling and decoding of archive control protocol response messages.
    /// </summary>
    public class ControlResponsePoller : IControlledFragmentHandler
    {
        /// <summary>
        /// Limit to apply when polling response messages.
        /// </summary>
        public const int FRAGMENT_LIMIT = 10;

        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();
        private readonly ChallengeDecoder challengeDecoder = new ChallengeDecoder();

        private readonly Subscription subscription;
        private ControlledFragmentAssembler fragmentAssembler;
        private long controlSessionId = Aeron.Aeron.NULL_VALUE;
        private long correlationId = Aeron.Aeron.NULL_VALUE;
        private long relevantId = Aeron.Aeron.NULL_VALUE;
        private int version = 0;
        private readonly int fragmentLimit;
        private ControlResponseCode code;
        private string errorMessage;
        private byte[] encodedChallenge = null;
        private bool isPollComplete = false;

        /// <summary>
        /// Create a poller for a given subscription to an archive for control response messages.
        /// </summary>
        /// <param name="subscription">  to poll for new events. </param>
        /// <param name="fragmentLimit"> to apply when polling. </param>
        private ControlResponsePoller(Subscription subscription, int fragmentLimit)
        {
            this.fragmentAssembler = new ControlledFragmentAssembler(this);

            this.subscription = subscription;
            this.fragmentLimit = fragmentLimit;
        }

        /// <summary>
        /// Create a poller for a given subscription to an archive for control response messages with a default
        /// fragment limit for polling as <seealso cref="FRAGMENT_LIMIT"/>.
        /// </summary>
        /// <param name="subscription">  to poll for new events. </param>
        public ControlResponsePoller(Subscription subscription) : this(subscription, FRAGMENT_LIMIT)
        {
            fragmentAssembler = new ControlledFragmentAssembler(this);
        }

        /// <summary>
        /// Get the <seealso cref="Subscription"/> used for polling responses.
        /// </summary>
        /// <returns> the <seealso cref="Subscription"/> used for polling responses. </returns>
        public Subscription Subscription()
        {
            return subscription;
        }

        /// <summary>
        /// Poll for control response events.
        /// </summary>
        /// <returns> the number of fragments read during the operation. Zero if no events are available. </returns>
        public int Poll()
        {
            controlSessionId = Aeron.Aeron.NULL_VALUE;
            correlationId = Aeron.Aeron.NULL_VALUE;
            relevantId = Aeron.Aeron.NULL_VALUE;
            version = 0;
            errorMessage = null;
            encodedChallenge = null;
            isPollComplete = false;

            return subscription.ControlledPoll(fragmentAssembler, fragmentLimit);
        }

        /// <summary>
        /// Control session id of the last polled message or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing.
        /// </summary>
        /// <returns> control session id of the last polled message or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing. </returns>
        public long ControlSessionId()
        {
            return controlSessionId;
        }

        /// <summary>
        /// Correlation id of the last polled message or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing.
        /// </summary>
        /// <returns> correlation id of the last polled message or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing. </returns>
        public long CorrelationId()
        {
            return correlationId;
        }

        /// <summary>
        /// Get the relevant id returned with the response, e.g. replay session id.
        /// </summary>
        /// <returns> the relevant id returned with the response. </returns>
        public long RelevantId()
        {
            return relevantId;
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
        /// Has the last polling action received a complete message?
        /// </summary>
        /// <returns> true if the last polling action received a complete message? </returns>
        public bool PollComplete
        {
            get { return isPollComplete; }
        }

        /// <summary>
        /// Get the response code of the last response.
        /// </summary>
        /// <returns> the response code of the last response. </returns>
        public ControlResponseCode Code()
        {
            return code;
        }

        /// <summary>
        /// Get the error message of the last response.
        /// </summary>
        /// <returns> the error message of the last response. </returns>
        public string ErrorMessage()
        {
            return errorMessage;
        }

        /// <summary>
        /// Was the last polling action received a challenge message?
        /// </summary>
        /// <returns> true if the last polling action received was a challenge message, false if not. </returns>
        public bool WasChallenged()
        {
            return null != encodedChallenge;
        }

        /// <summary>
        /// Get the encoded challenge of the last challenge.
        /// </summary>
        /// <returns> the encoded challenge of the last challenge. </returns>
        public byte[] EncodedChallenge()
        {
            return encodedChallenge;
        }

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
                throw new ArchiveException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" +
                                           schemaId);
            }

            if (messageHeaderDecoder.TemplateId() == ControlResponseDecoder.TEMPLATE_ID)
            {
                controlResponseDecoder.Wrap(buffer, offset + MessageHeaderEncoder.ENCODED_LENGTH,
                    messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                controlSessionId = controlResponseDecoder.ControlSessionId();
                correlationId = controlResponseDecoder.CorrelationId();
                relevantId = controlResponseDecoder.RelevantId();
                code = controlResponseDecoder.Code();
                version = controlResponseDecoder.Version();
                errorMessage = controlResponseDecoder.ErrorMessage();
                isPollComplete = true;

                return BREAK;
            }

            if (messageHeaderDecoder.TemplateId() == ChallengeDecoder.TEMPLATE_ID)
            {
                challengeDecoder.Wrap(buffer, offset + MessageHeaderEncoder.ENCODED_LENGTH,
                    messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                controlSessionId = challengeDecoder.ControlSessionId();
                correlationId = challengeDecoder.CorrelationId();
                relevantId = Aeron.Aeron.NULL_VALUE;
                code = ControlResponseCode.NULL_VALUE;
                version = challengeDecoder.Version();
                errorMessage = "";

                int encodedChallengeLength = challengeDecoder.EncodedChallengeLength();
                encodedChallenge = new byte[encodedChallengeLength];
                challengeDecoder.GetEncodedChallenge(encodedChallenge, 0, encodedChallengeLength);

                isPollComplete = true;

                return BREAK;
            }

            return CONTINUE;
        }

        public override string ToString()
        {
            return "ControlResponsePoller{" +
                   "controlSessionId=" + controlSessionId +
                   ", correlationId=" + correlationId +
                   ", relevantId=" + relevantId +
                   ", code=" + code +
                   ", version=" + SemanticVersion.ToString(version) +
                   ", errorMessage='" + errorMessage + '\'' +
                   ", isPollComplete=" + isPollComplete +
                   '}';
        }
    }
}