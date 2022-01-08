using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Archiver.Codecs;
using static Adaptive.Aeron.LogBuffer.ControlledFragmentHandlerAction;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Encapsulate the polling, decoding, dispatching of recording descriptors from an archive.
    /// </summary>
    /// <seealso cref="IRecordingSubscriptionDescriptorConsumer"></seealso>
    /// <seealso cref="ArchiveProxy.ListRecordingSubscriptions(int, int, string, int, bool, long, long)"></seealso>
    /// <seealso cref="AeronArchive.ListRecordingSubscriptions(int, int, string, int, bool, IRecordingSubscriptionDescriptorConsumer)"></seealso>
    public class RecordingSubscriptionDescriptorPoller : IControlledFragmentHandler
    {
        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();

        private readonly RecordingSubscriptionDescriptorDecoder recordingSubscriptionDescriptorDecoder =
            new RecordingSubscriptionDescriptorDecoder();

        private readonly RecordingSignalEventDecoder recordingSignalEventDecoder = new RecordingSignalEventDecoder();

        private readonly long controlSessionId;
        private readonly int fragmentLimit;
        private readonly Subscription subscription;
        private readonly ControlledFragmentAssembler fragmentAssembler;
        private readonly ErrorHandler errorHandler;
        private readonly IRecordingSignalConsumer recordingSignalConsumer;

        private long correlationId;
        private int remainingSubscriptionCount;
        private bool isDispatchComplete = false;
        private IRecordingSubscriptionDescriptorConsumer subscriptionDescriptorConsumer;

        /// <summary>
        /// Create a poller for a given subscription to an archive for control response messages.
        /// </summary>
        /// <param name="subscription">     to poll for new events. </param>
        /// <param name="errorHandler">     to call for asynchronous errors. </param>
        /// <param name="controlSessionId"> to filter the responses. </param>
        /// <param name="fragmentLimit">    to apply for each polling operation. </param>
        public RecordingSubscriptionDescriptorPoller(
            Subscription subscription,
            ErrorHandler errorHandler,
            long controlSessionId,
            int fragmentLimit) :
            this(
                subscription,
                errorHandler,
                AeronArchive.Configuration.NO_OP_RECORDING_SIGNAL_CONSUMER,
                controlSessionId,
                fragmentLimit)
        {
        }


        /// <summary>
        /// Create a poller for a given subscription to an archive for control response messages.
        /// </summary>
        /// <param name="subscription">     to poll for new events. </param>
        /// <param name="errorHandler">     to call for asynchronous errors. </param>
        /// <param name="recordingSignalConsumer">  for consuming interleaved recording signals on the control session.</param>
        /// <param name="controlSessionId"> to filter the responses. </param>
        /// <param name="fragmentLimit">    to apply for each polling operation. </param>
        public RecordingSubscriptionDescriptorPoller(
            Subscription subscription,
            ErrorHandler errorHandler,
            IRecordingSignalConsumer recordingSignalConsumer,
            long controlSessionId,
            int fragmentLimit)
        {
            this.fragmentAssembler = new ControlledFragmentAssembler(this);
            this.subscription = subscription;
            this.errorHandler = errorHandler;
            this.recordingSignalConsumer = recordingSignalConsumer;
            this.fragmentLimit = fragmentLimit;
            this.controlSessionId = controlSessionId;
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
        /// Poll for recording subscriptions and delegate to the <seealso cref="IRecordingSubscriptionDescriptorConsumer"/>.
        /// </summary>
        /// <returns> the number of fragments read during the operation. Zero if no events are available. </returns>
        public int Poll()
        {
            if (isDispatchComplete)
            {
                isDispatchComplete = false;
            }

            return subscription.ControlledPoll(fragmentAssembler, fragmentLimit);
        }

        /// <summary>
        /// Control session id for filtering responses.
        /// </summary>
        /// <returns> control session id for filtering responses. </returns>
        public long ControlSessionId()
        {
            return controlSessionId;
        }

        /// <summary>
        /// Is the dispatch of descriptors complete?
        /// </summary>
        /// <returns> true if the dispatch of descriptors complete? </returns>
        public bool DispatchComplete
        {
            get { return isDispatchComplete; }
        }

        /// <summary>
        /// Get the number of remaining subscriptions expected.
        /// </summary>
        /// <returns> the number of remaining subscriptions expected. </returns>
        public int RemainingSubscriptionCount()
        {
            return remainingSubscriptionCount;
        }

        /// <summary>
        /// Reset the poller to dispatch the descriptors returned from a query.
        /// </summary>
        /// <param name="correlationId">     for the response. </param>
        /// <param name="subscriptionCount"> of descriptors to expect. </param>
        /// <param name="consumer">          to which the recording subscription descriptors are to be dispatched. </param>
        public void Reset(long correlationId, int subscriptionCount, IRecordingSubscriptionDescriptorConsumer consumer)
        {
            this.correlationId = correlationId;
            this.subscriptionDescriptorConsumer = consumer;
            this.remainingSubscriptionCount = subscriptionCount;
            isDispatchComplete = false;
        }

        public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            if (isDispatchComplete)
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
                {
                    controlResponseDecoder
                        .Wrap(buffer,
                            offset + MessageHeaderEncoder.ENCODED_LENGTH,
                            messageHeaderDecoder.BlockLength(),
                            messageHeaderDecoder.Version());

                    if (controlResponseDecoder.ControlSessionId() == controlSessionId)
                    {
                        ControlResponseCode code = controlResponseDecoder.Code();
                        long correlationId = controlResponseDecoder.CorrelationId();

                        if (ControlResponseCode.SUBSCRIPTION_UNKNOWN == code && correlationId == this.correlationId)
                        {
                            isDispatchComplete = true;
                            return BREAK;
                        }

                        if (ControlResponseCode.ERROR == code)
                        {
                            ArchiveException ex = new ArchiveException(
                                "response for correlationId=" + this.correlationId + ", error: " +
                                controlResponseDecoder.ErrorMessage(), (int)controlResponseDecoder.RelevantId(),
                                correlationId);

                            if (correlationId == this.correlationId)
                            {
                                throw ex;
                            }
                            else
                            {
                                errorHandler?.Invoke(ex);
                            }
                        }
                    }
                }

                    break;

                case RecordingSubscriptionDescriptorDecoder.TEMPLATE_ID:
                {
                    recordingSubscriptionDescriptorDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderEncoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(),
                        messageHeaderDecoder.Version());

                    if (recordingSubscriptionDescriptorDecoder.ControlSessionId() == controlSessionId &&
                        recordingSubscriptionDescriptorDecoder.CorrelationId() == this.correlationId)
                    {
                        subscriptionDescriptorConsumer.OnSubscriptionDescriptor(
                            controlSessionId,
                            recordingSubscriptionDescriptorDecoder.CorrelationId(),
                            recordingSubscriptionDescriptorDecoder.SubscriptionId(),
                            recordingSubscriptionDescriptorDecoder.StreamId(),
                            recordingSubscriptionDescriptorDecoder.StrippedChannel());

                        if (0 == --remainingSubscriptionCount)
                        {
                            isDispatchComplete = true;
                            return BREAK;
                        }
                    }
                }
                    break;

                case RecordingSignalEventDecoder.TEMPLATE_ID:
                    recordingSignalEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(),
                        messageHeaderDecoder.Version());

                    if (controlSessionId == recordingSignalEventDecoder.ControlSessionId())
                    {
                        recordingSignalConsumer.OnSignal(
                            recordingSignalEventDecoder.ControlSessionId(),
                            recordingSignalEventDecoder.CorrelationId(),
                            recordingSignalEventDecoder.RecordingId(),
                            recordingSignalEventDecoder.SubscriptionId(),
                            recordingSignalEventDecoder.Position(),
                            recordingSignalEventDecoder.Signal());
                    }

                    break;
            }

            return CONTINUE;
        }
    }
}