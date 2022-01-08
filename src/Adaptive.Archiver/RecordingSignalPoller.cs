using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Archiver.Codecs;
using ControlledFragmentAssembler = Adaptive.Aeron.ControlledFragmentAssembler;
using Subscription = Adaptive.Aeron.Subscription;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Encapsulate the polling and decoding of archive control protocol response and recording signal messages.
    /// </summary>
    public sealed class RecordingSignalPoller : IControlledFragmentHandler
    {
        /// <summary>
        /// Limit to apply when polling messages.
        /// </summary>
        public const int FRAGMENT_LIMIT = 10;

        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();
        private readonly RecordingSignalEventDecoder recordingSignalEventDecoder = new RecordingSignalEventDecoder();

        private readonly Subscription subscription;
        private ControlledFragmentAssembler fragmentAssembler;
        private readonly long controlSessionId;
        private long correlationId = Aeron.Aeron.NULL_VALUE;
        private long relevantId = Aeron.Aeron.NULL_VALUE;
        private int templateId = Aeron.Aeron.NULL_VALUE;
        private int version = 0;
        private long recordingId = Aeron.Aeron.NULL_VALUE;
        private long recordingSubscriptionId = Aeron.Aeron.NULL_VALUE;
        private long recordingPosition = Aeron.Aeron.NULL_VALUE;
        private RecordingSignal recordingSignal = Codecs.RecordingSignal.NULL_VALUE;
        private ControlResponseCode code;
        private string errorMessage;
        private readonly int fragmentLimit;
        private bool isPollComplete = false;

        /// <summary>
        /// Create a poller for a given subscription to an archive for control messages.
        /// </summary>
        /// <param name="controlSessionId"> to listen for associated asynchronous control events, such as errors. </param>
        /// <param name="subscription">     to poll for new events. </param>
        /// <param name="fragmentLimit">    to apply when polling. </param>
        private RecordingSignalPoller(long controlSessionId, Subscription subscription, int fragmentLimit)
        {
            fragmentAssembler = new ControlledFragmentAssembler(this);

            this.controlSessionId = controlSessionId;
            this.subscription = subscription;
            this.fragmentLimit = fragmentLimit;
        }

        /// <summary>
        /// Create a poller for a given subscription to an archive for control response messages with a default
        /// fragment limit for polling as <seealso cref="FRAGMENT_LIMIT"/>.
        /// </summary>
        /// <param name="controlSessionId"> to listen for associated asynchronous control events, such as errors. </param>
        /// <param name="subscription"> to poll for new events. </param>
        public RecordingSignalPoller(long controlSessionId, Subscription subscription) 
            : this(controlSessionId, subscription, FRAGMENT_LIMIT)
        {
        }

        /// <summary>
        /// Get the <seealso cref="Subscription"/> used for polling messages.
        /// </summary>
        /// <returns> the <seealso cref="Subscription"/> used for polling messages. </returns>
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
                correlationId = Aeron.Aeron.NULL_VALUE;
                relevantId = Aeron.Aeron.NULL_VALUE;
                version = 0;
                errorMessage = null;
                recordingId = Aeron.Aeron.NULL_VALUE;
                recordingSubscriptionId = Aeron.Aeron.NULL_VALUE;
                recordingPosition = Aeron.Aeron.NULL_VALUE;
                recordingSignal = Codecs.RecordingSignal.NULL_VALUE;
            }

            return subscription.ControlledPoll(fragmentAssembler, fragmentLimit);
        }

        /// <summary>
        /// Control session id of the last polled message or <seealso cref="Aeron.Aeron.NULL_VALUE"/> if poll returned nothing.
        /// </summary>
        /// <returns> control session id of the last polled message or <seealso cref="Aeron.Aeron.NULL_VALUE"/> if poll returned nothing. </returns>
        public long ControlSessionId()
        {
            return controlSessionId;
        }

        /// <summary>
        /// Correlation id of the last polled message or <seealso cref="Aeron.Aeron.NULL_VALUE"/> if poll returned nothing.
        /// </summary>
        /// <returns> correlation id of the last polled message or <seealso cref="Aeron.Aeron.NULL_VALUE"/> if poll returned nothing. </returns>
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
        /// Get the template id of the last received message.
        /// </summary>
        /// <returns> the template id of the last received message. </returns>
        public int TemplateId()
        {
            return templateId;
        }

        /// <summary>
        /// Get the recording id of the last received message.
        /// </summary>
        /// <returns> the recording id of the last received message. </returns>
        public long RecordingId()
        {
            return recordingId;
        }

        /// <summary>
        /// Get the recording subscription id of the last received message.
        /// </summary>
        /// <returns> the recording subscription id of the last received message. </returns>
        public long RecordingSubscriptionId()
        {
            return recordingSubscriptionId;
        }

        /// <summary>
        /// Get the recording position of the last received message.
        /// </summary>
        /// <returns> the recording position of the last received message. </returns>
        public long RecordingPosition()
        {
            return recordingPosition;
        }

        /// <summary>
        /// Get the recording signal of the last received message.
        /// </summary>
        /// <returns> the recording signal of the last received message. </returns>
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
        /// Get the error message of the last response.
        /// </summary>
        /// <returns> the error message of the last response. </returns>
        public string ErrorMessage()
        {
            return errorMessage;
        }

        public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            if (isPollComplete)
            {
                return ControlledFragmentHandlerAction.ABORT;
            }

            messageHeaderDecoder.Wrap(buffer, offset);

            int schemaId = messageHeaderDecoder.SchemaId();
            if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
            {
                throw new ArchiveException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" +
                                           schemaId);
            }

            int templateId = messageHeaderDecoder.TemplateId();

            if (ControlResponseDecoder.TEMPLATE_ID == templateId)
            {
                controlResponseDecoder.Wrap(
                    buffer, 
                    offset + MessageHeaderEncoder.ENCODED_LENGTH,
                    messageHeaderDecoder.BlockLength(), 
                    messageHeaderDecoder.Version());

                if (controlResponseDecoder.ControlSessionId() == controlSessionId)
                {
                    this.templateId = templateId;
                    correlationId = controlResponseDecoder.CorrelationId();
                    relevantId = controlResponseDecoder.RelevantId();
                    code = controlResponseDecoder.Code();
                    version = controlResponseDecoder.Version();
                    errorMessage = controlResponseDecoder.ErrorMessage();
                    isPollComplete = true;

                    return ControlledFragmentHandlerAction.BREAK;
                }
            }
            else if (RecordingSignalEventDecoder.TEMPLATE_ID == templateId)
            {
                recordingSignalEventDecoder.Wrap(
                    buffer, 
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.BlockLength(),
                    messageHeaderDecoder.Version());

                if (recordingSignalEventDecoder.ControlSessionId() == controlSessionId)
                {
                    this.templateId = templateId;
                    correlationId = recordingSignalEventDecoder.CorrelationId();
                    recordingId = recordingSignalEventDecoder.RecordingId();
                    recordingSubscriptionId = recordingSignalEventDecoder.SubscriptionId();
                    recordingPosition = recordingSignalEventDecoder.Position();
                    recordingSignal = recordingSignalEventDecoder.Signal();
                    isPollComplete = true;

                    return ControlledFragmentHandlerAction.BREAK;
                }
            }

            return ControlledFragmentHandlerAction.CONTINUE;
        }

        /// <summary>
        /// {@inheritDoc}
        /// </summary>
        public override string ToString()
        {
            return "RecordingSignalPoller{" +
                   "controlSessionId=" + controlSessionId +
                   ", correlationId=" + correlationId +
                   ", relevantId=" + relevantId +
                   ", code=" + code +
                   ", templateId=" + templateId +
                   ", version=" + SemanticVersion.ToString(version) +
                   ", errorMessage='" + errorMessage + '\'' +
                   ", recordingId=" + recordingId +
                   ", recordingSubscriptionId=" + recordingSubscriptionId +
                   ", recordingPosition=" + recordingPosition +
                   ", recordingSignal=" + recordingSignal +
                   ", isPollComplete=" + isPollComplete +
                   '}';
        }
    }
}