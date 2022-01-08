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
        private readonly RecordingSignalEventDecoder recordingSignalEventDecoder = new RecordingSignalEventDecoder();

        private readonly Subscription subscription;
        private ControlledFragmentAssembler fragmentAssembler;
        private long controlSessionId = Aeron.Aeron.NULL_VALUE;
        private long correlationId = Aeron.Aeron.NULL_VALUE;
        private long relevantId = Aeron.Aeron.NULL_VALUE;
        private int templateId = Aeron.Aeron.NULL_VALUE;
        private int version = 0;
        private readonly int fragmentLimit;
        private ControlResponseCode code;
        private string errorMessage;
        private long recordingId = Aeron.Aeron.NULL_VALUE;
        private long subscriptionId = Aeron.Aeron.NULL_VALUE;
        private long position = Aeron.Aeron.NULL_VALUE;
        private RecordingSignal recordingSignal = Codecs.RecordingSignal.NULL_VALUE;
        private byte[] encodedChallenge = null;
        private bool isPollComplete = false;

        /// <summary>
        /// Create a poller for a given subscription to an archive for control response messages with a default
        /// fragment limit for polling as <seealso cref="FRAGMENT_LIMIT"/>.
        /// </summary>
        /// <param name="subscription">  to poll for new events. </param>
        public ControlResponsePoller(Subscription subscription) : this(subscription, FRAGMENT_LIMIT)
        {
        }

        /// <summary>
        /// Create a poller for a given subscription to an archive for control response messages.
        /// </summary>
        /// <param name="subscription">  to poll for new events. </param>
        /// <param name="fragmentLimit"> to apply when polling. </param>
        public ControlResponsePoller(Subscription subscription, int fragmentLimit)
        {
            this.fragmentAssembler = new ControlledFragmentAssembler(this);

            this.subscription = subscription;
            this.fragmentLimit = fragmentLimit;
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
            if (isPollComplete)
            {
                isPollComplete = false;
                templateId = Aeron.Aeron.NULL_VALUE;
                controlSessionId = Aeron.Aeron.NULL_VALUE;
                correlationId = Aeron.Aeron.NULL_VALUE;
                relevantId = Aeron.Aeron.NULL_VALUE;
                recordingId = Aeron.Aeron.NULL_VALUE;
                subscriptionId = Aeron.Aeron.NULL_VALUE;
                position = Aeron.Aeron.NULL_VALUE;
                recordingSignal = Codecs.RecordingSignal.NULL_VALUE;
                version = 0;
                errorMessage = null;
                encodedChallenge = null;
            }

            return subscription.ControlledPoll(fragmentAssembler, fragmentLimit);
        }

        /// <summary>
        /// SBE template id of polled message or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing.
        /// </summary>
        /// <returns> SBE template id of polled message or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing. </returns>
        public int TemplateId()
        {
            return templateId;
        }
        
        /// <summary>
        /// Control session id of polled message or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing.
        /// </summary>
        /// <returns> control session id of polled message or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing. </returns>
        public long ControlSessionId()
        {
            return controlSessionId;
        }

        /// <summary>
        /// Correlation id of the message or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing.
        /// </summary>
        /// <returns> correlation id of polled message or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing. </returns>
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
        /// Recording id of polled <seealso cref="RecordingSignal"/> or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing.
        /// </summary>
        /// <returns> recording id of polled <seealso cref="RecordingSignal"/> or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing. </returns>
        public long RecordingId()
        {
            return recordingId;
        }

        /// <summary>
        /// Subscription id of polled <seealso cref="RecordingSignal"/> or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing.
        /// </summary>
        /// <returns> subscription id of polled <seealso cref="RecordingSignal"/> or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing. </returns>
        public long SubscriptionId()
        {
            return subscriptionId;
        }

        /// <summary>
        /// Position of polled <seealso cref="RecordingSignal"/> or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing.
        /// </summary>
        /// <returns> position of polled <seealso cref="RecordingSignal"/> or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing. </returns>
        public long Position()
        {
            return position;
        }

        /// <summary>
        /// Enum of polled <seealso cref="RecordingSignal"/> or null if poll returned nothing.
        /// </summary>
        /// <returns> enum of polled <seealso cref="RecordingSignal"/> or null if poll returned nothing. </returns>
        public RecordingSignal RecordingSignal()
        {
            return recordingSignal;
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
        /// Get the error message of the response.
        /// </summary>
        /// <returns> the error message of the response. </returns>
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

            templateId = messageHeaderDecoder.TemplateId();
            switch (templateId)
            {
                case ControlResponseDecoder.TEMPLATE_ID:
                {
                    controlResponseDecoder.Wrap(
                        buffer, 
                        offset + MessageHeaderEncoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), 
                        messageHeaderDecoder.Version());

                    controlSessionId = controlResponseDecoder.ControlSessionId();
                    correlationId = controlResponseDecoder.CorrelationId();
                    relevantId = controlResponseDecoder.RelevantId();
                    code = controlResponseDecoder.Code();
                    version = controlResponseDecoder.Version();
                    errorMessage = controlResponseDecoder.ErrorMessage();
                    isPollComplete = true;
                    
                    return BREAK;
                }

                case ChallengeDecoder.TEMPLATE_ID:
                {
                    challengeDecoder.Wrap(
                        buffer, 
                        offset + MessageHeaderEncoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), 
                        messageHeaderDecoder.Version());

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
                
                case RecordingSignalEventDecoder.TEMPLATE_ID:
                {
                    recordingSignalEventDecoder.Wrap(
                        buffer, 
                        offset + MessageHeaderDecoder.ENCODED_LENGTH, 
                        messageHeaderDecoder.BlockLength(), 
                        messageHeaderDecoder.Version());

                    controlSessionId = recordingSignalEventDecoder.ControlSessionId();
                    correlationId = recordingSignalEventDecoder.CorrelationId();
                    recordingId = recordingSignalEventDecoder.RecordingId();
                    subscriptionId = recordingSignalEventDecoder.SubscriptionId();
                    position = recordingSignalEventDecoder.Position();
                    recordingSignal = recordingSignalEventDecoder.Signal();

                    isPollComplete = true;

                    return BREAK;
                }

                
                default:
                    return CONTINUE;
            }
        }

        public override string ToString()
        {
            return "ControlResponsePoller{" +
                   "templateId=" + templateId +
                   ", controlSessionId=" + controlSessionId +
                   ", correlationId=" + correlationId +
                   ", relevantId=" + relevantId +
                   ", recordingId=" + recordingId +
                   ", subscriptionId=" + subscriptionId +
                   ", position=" + position +
                   ", recordingSignal=" + recordingSignal +
                   ", code=" + code +
                   ", version=" + SemanticVersion.ToString(version) +
                   ", errorMessage='" + errorMessage + '\'' +
                   ", isPollComplete=" + isPollComplete +
                   '}';
        }
    }
}