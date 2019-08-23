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

        private readonly Subscription subscription;
        private ControlledFragmentAssembler fragmentAssembler;
        private long controlSessionId = Aeron.Aeron.NULL_VALUE;
        private long correlationId = Aeron.Aeron.NULL_VALUE;
        private long relevantId = Aeron.Aeron.NULL_VALUE;
        private int templateId = Aeron.Aeron.NULL_VALUE;
        private readonly int fragmentLimit;
        private ControlResponseCode code;
        private string errorMessage;
        private bool pollComplete = false;

        /// <summary>
        /// Create a poller for a given subscription to an archive for control response messages.
        /// </summary>
        /// <param name="subscription">  to poll for new events. </param>
        /// <param name="fragmentLimit"> to apply when polling.</param>
        public ControlResponsePoller(Subscription subscription, int fragmentLimit = FRAGMENT_LIMIT)
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
            controlSessionId = -1;
            correlationId = -1;
            relevantId = -1;
            templateId = -1;
            errorMessage = null;
            pollComplete = false;

            return subscription.ControlledPoll(fragmentAssembler, fragmentLimit);
        }

        /// <summary>
        /// Control session id of the last polled message or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing.
        /// </summary>
        /// <returns> control session id of the last polled message or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing. </returns>
        public long ControlSessionId()
        {
            return controlSessionId;
        }

        /// <summary>
        /// Correlation id of the last polled message or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing.
        /// </summary>
        /// <returns> correlation id of the last polled message or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing. </returns>
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
        /// Has the last polling action received a complete message?
        /// </summary>
        /// <returns> true if the last polling action received a complete message? </returns>
        public bool IsPollComplete()
        {
            return pollComplete;
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
            if (pollComplete)
            {
                return ABORT;
            }

            messageHeaderDecoder.Wrap(buffer, offset);

            int schemaId = messageHeaderDecoder.SchemaId();
            if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
            {
                throw new ArchiveException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
            }

            templateId = messageHeaderDecoder.TemplateId();
            if (templateId == ControlResponseDecoder.TEMPLATE_ID)
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
                errorMessage = controlResponseDecoder.ErrorMessage();
                pollComplete = true;

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
                   ", templateId=" + templateId +
                   ", code=" + code +
                   ", errorMessage='" + errorMessage + '\'' +
                   ", pollComplete=" + pollComplete +
                   '}';
        }
    }
}