using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Archiver.Codecs;
using static Adaptive.Aeron.LogBuffer.ControlledFragmentHandlerAction;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Encapsulate the polling, decoding, and dispatching of recording transition events for a session plus the
    /// asynchronous events to check for errors.
    /// </summary>
    /// <seealso cref="RecordingSignal"></seealso>
    public class RecordingSignalAdapter : IControlledFragmentHandler
    {
        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();
        private readonly RecordingSignalEventDecoder recordingSignalEventDecoder = new RecordingSignalEventDecoder();
        private ControlledFragmentAssembler assembler;
        private readonly IControlEventListener controlEventListener;
        private readonly IRecordingSignalConsumer recordingSignalConsumer;
        private readonly Subscription subscription;
        private readonly int fragmentLimit;
        private readonly long controlSessionId;
        private bool isDone = false;

        /// <summary>
        /// Create an adapter for a given subscription to an archive for recording events.
        /// </summary>
        /// <param name="controlSessionId">            to listen for associated asynchronous control events, such as errors. </param>
        /// <param name="controlEventListener">        listener for control events which may indicate an error on the session. </param>
        /// <param name="recordingSignalConsumer"> consumer of recording transition events. </param>
        /// <param name="subscription">                to poll for new events. </param>
        /// <param name="fragmentLimit">               to apply for each polling operation. </param>
        public RecordingSignalAdapter(long controlSessionId, IControlEventListener controlEventListener,
            IRecordingSignalConsumer recordingSignalConsumer, Subscription subscription, int fragmentLimit)
        {
            assembler = new ControlledFragmentAssembler(this);

            this.controlSessionId = controlSessionId;
            this.controlEventListener = controlEventListener;
            this.recordingSignalConsumer = recordingSignalConsumer;
            this.subscription = subscription;
            this.fragmentLimit = fragmentLimit;
        }

        /// <summary>
        /// Poll for recording transitions and dispatch them to the <seealso cref="IRecordingSignalConsumer"/> for this instance,
        /// plus check for async responses for this control session which may have an exception and dispatch to the
        /// <seealso cref="IControlResponseListener"/>.
        /// </summary>
        /// <returns> the number of fragments read during the operation. Zero if no events are available. </returns>
        public int Poll()
        {
            isDone = false;
            return subscription.ControlledPoll(assembler, fragmentLimit);
        }

        /// <summary>
        /// Indicate that poll was successful and a signal or control response was received.
        /// </summary>
        /// <returns> true if a signal or control response was received. </returns>
        public bool Done
        {
            get { return isDone; }
        }

        public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            if (isDone)
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

            int templateId = messageHeaderDecoder.TemplateId();
            switch (templateId)
            {
                case ControlResponseDecoder.TEMPLATE_ID:
                    controlResponseDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    if (controlResponseDecoder.ControlSessionId() == controlSessionId)
                    {
                        controlEventListener.OnResponse(controlSessionId, controlResponseDecoder.CorrelationId(),
                            controlResponseDecoder.RelevantId(), controlResponseDecoder.Code(),
                            controlResponseDecoder.ErrorMessage());

                        isDone = true;
                        return BREAK;
                    }

                    break;

                case RecordingSignalEventDecoder.TEMPLATE_ID:
                    recordingSignalEventDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    if (recordingSignalEventDecoder.ControlSessionId() == controlSessionId)
                    {
                        recordingSignalConsumer.OnSignal(recordingSignalEventDecoder.ControlSessionId(),
                            recordingSignalEventDecoder.CorrelationId(), recordingSignalEventDecoder.RecordingId(),
                            recordingSignalEventDecoder.SubscriptionId(), recordingSignalEventDecoder.Position(),
                            recordingSignalEventDecoder.Signal());

                        isDone = true;
                        return BREAK;
                    }

                    break;
            }

            return CONTINUE;
        }
    }
}