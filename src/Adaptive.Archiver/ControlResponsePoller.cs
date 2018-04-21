using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Archiver.Codecs;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Encapsulate the polling and decoding of archive control protocol response messages.
    /// </summary>
    public class ControlResponsePoller : IControlledFragmentHandler
    {
        private bool InstanceFieldsInitialized = false;

        private void InitializeInstanceFields()
        {
            fragmentAssembler = new ControlledFragmentAssembler(this);
        }

        private const int FRAGMENT_LIMIT = 10;

        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();

        private readonly Subscription subscription;
        private ControlledFragmentAssembler fragmentAssembler;
        private long controlSessionId = -1;
        private long correlationId = -1;
        private long relevantId = -1;
        private int templateId = -1;
        private ControlResponseCode code;
        private string errorMessage;
        private bool pollComplete = false;

        /// <summary>
        /// Create a poller for a given subscription to an archive for control response messages.
        /// </summary>
        /// <param name="subscription">  to poll for new events. </param>
        public ControlResponsePoller(Subscription subscription)
        {
            if (!InstanceFieldsInitialized)
            {
                InitializeInstanceFields();
                InstanceFieldsInitialized = true;
            }

            this.subscription = subscription;
        }

        /// <summary>
        /// Get the <seealso cref="Subscription"/> used for polling responses.
        /// </summary>
        /// <returns> the <seealso cref="Subscription"/> used for polling responses. </returns>
        public virtual Subscription Subscription()
        {
            return subscription;
        }

        /// <summary>
        /// Poll for recording events.
        /// </summary>
        /// <returns> the number of fragments read during the operation. Zero if no events are available. </returns>
        public virtual int Poll()
        {
            controlSessionId = -1;
            correlationId = -1;
            relevantId = -1;
            templateId = -1;
            pollComplete = false;

            return subscription.ControlledPoll(fragmentAssembler, FRAGMENT_LIMIT);
        }

        /// <summary>
        /// Control session id of the last polled message or -1 if poll returned nothing.
        /// </summary>
        /// <returns> control session id of the last polled message or -1 if unrecognised template. </returns>
        public virtual long ControlSessionId()
        {
            return controlSessionId;
        }

        /// <summary>
        /// Correlation id of the last polled message or -1 if poll returned nothing.
        /// </summary>
        /// <returns> correlation id of the last polled message or -1 if unrecognised template. </returns>
        public virtual long CorrelationId()
        {
            return correlationId;
        }

        /// <summary>
        /// Get the relevant id returned with the response, e.g. replay session id.
        /// </summary>
        /// <returns> the relevant id returned with the response. </returns>
        public virtual long RelevantId()
        {
            return relevantId;
        }

        /// <summary>
        /// Has the last polling action received a complete message?
        /// </summary>
        /// <returns> true if the last polling action received a complete message? </returns>
        public virtual bool IsPollComplete()
        {
            return pollComplete;
        }

        /// <summary>
        /// Get the template id of the last received message.
        /// </summary>
        /// <returns> the template id of the last received message. </returns>
        public virtual int TemplateId()
        {
            return templateId;
        }

        /// <summary>
        /// Get the response code of the last response.
        /// </summary>
        /// <returns> the response code of the last response. </returns>
        public virtual ControlResponseCode Code()
        {
            return code;
        }

        /// <summary>
        /// Get the error message of the last response.
        /// </summary>
        /// <returns> the error message of the last response. </returns>
        public virtual string ErrorMessage()
        {
            return errorMessage;
        }

        public virtual ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            messageHeaderDecoder.Wrap(buffer, offset);

            templateId = messageHeaderDecoder.TemplateId();
            switch (templateId)
            {
                case ControlResponseDecoder.TEMPLATE_ID:
                    controlResponseDecoder.Wrap(buffer, offset + MessageHeaderEncoder.ENCODED_LENGTH, messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    controlSessionId = controlResponseDecoder.ControlSessionId();
                    correlationId = controlResponseDecoder.CorrelationId();
                    relevantId = controlResponseDecoder.RelevantId();
                    code = controlResponseDecoder.Code();
                    if (ControlResponseCode.ERROR == code)
                    {
                        errorMessage = controlResponseDecoder.ErrorMessage();
                    }
                    else
                    {
                        errorMessage = "";
                    }

                    break;

                case RecordingDescriptorDecoder.TEMPLATE_ID:
                    break;

                default:
                    throw new System.InvalidOperationException("unknown templateId: " + templateId);
            }

            pollComplete = true;

            return ControlledFragmentHandlerAction.BREAK;
        }
    }
}