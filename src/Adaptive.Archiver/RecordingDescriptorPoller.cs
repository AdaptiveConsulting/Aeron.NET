using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Archiver.Codecs;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Encapsulate the polling, decoding, dispatching of recording descriptors from an archive.
    /// </summary>
    public class RecordingDescriptorPoller : IControlledFragmentHandler
    {
        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();
        private readonly RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();
        private readonly RecordingSignalEventDecoder recordingSignalEventDecoder = new RecordingSignalEventDecoder();

        private readonly long controlSessionId;
        private readonly int fragmentLimit;
        private readonly Subscription subscription;
        private readonly ControlledFragmentAssembler fragmentAssembler;
        private readonly IErrorHandler errorHandler;
        private readonly IRecordingSignalConsumer recordingSignalConsumer;

        private long correlationId;
        private int remainingRecordCount;
        private bool isDispatchComplete = false;
        private IRecordingDescriptorConsumer recordingDescriptorConsumer;

        /// <summary>
        /// Create a poller for a given subscription to an archive for control response messages.
        /// </summary>
        /// <param name="subscription">     to poll for new events. </param>
        /// <param name="errorHandler">     to call for asynchronous errors. </param>
        /// <param name="controlSessionId"> to filter the responses. </param>
        /// <param name="fragmentLimit">    to apply for each polling operation. </param>
        public RecordingDescriptorPoller(
            Subscription subscription,
            IErrorHandler errorHandler, 
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
        /// <param name="errorHandler">     to call for asynchronous errors.</param>
        /// <param name="recordingSignalConsumer"> for consuming interleaved recording signals on the control session.</param>
        /// <param name="controlSessionId"> to filter the responses. </param>
        /// <param name="fragmentLimit">    to apply for each polling operation. </param>
        public RecordingDescriptorPoller(
            Subscription subscription,
            IErrorHandler errorHandler,
            IRecordingSignalConsumer recordingSignalConsumer,
            long controlSessionId,
            int fragmentLimit)
        {
            this.subscription = subscription;
            this.errorHandler = errorHandler;
            this.recordingSignalConsumer = recordingSignalConsumer;
            this.fragmentLimit = fragmentLimit;
            this.controlSessionId = controlSessionId;

            this.fragmentAssembler = new ControlledFragmentAssembler(this);
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
        /// Poll for recording events.
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
        public bool IsDispatchComplete()
        {
            return isDispatchComplete;
        }

        /// <summary>
        /// Get the number of remaining records are expected.
        /// </summary>
        /// <returns> the number of remaining records are expected. </returns>
        public int RemainingRecordCount()
        {
            return remainingRecordCount;
        }

        /// <summary>
        /// Reset the poller to dispatch the descriptors returned from a query.
        /// </summary>
        /// <param name="correlationId"> for the response. </param>
        /// <param name="recordCount">           of descriptors to expect. </param>
        /// <param name="consumer">              to which the recording descriptors are to be dispatched. </param>
        public void Reset(long correlationId, int recordCount, IRecordingDescriptorConsumer consumer)
        {
            this.correlationId = correlationId;
            this.recordingDescriptorConsumer = consumer;
            this.remainingRecordCount = recordCount;
            isDispatchComplete = false;
        }

        public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            if (isDispatchComplete)
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
            switch (templateId)
            {
                case ControlResponseDecoder.TEMPLATE_ID:
                {
                    controlResponseDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderEncoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(),
                        messageHeaderDecoder.Version());

                    if (controlResponseDecoder.ControlSessionId() == controlSessionId)
                    {
                        ControlResponseCode code = controlResponseDecoder.Code();
                        long responseCorrelationId = controlResponseDecoder.CorrelationId();

                        if (ControlResponseCode.RECORDING_UNKNOWN == code && responseCorrelationId == this.correlationId)
                        {
                            isDispatchComplete = true;
                            return ControlledFragmentHandlerAction.BREAK;
                        }

                        if (ControlResponseCode.ERROR == code)
                        {
                            ArchiveException ex = new ArchiveException(
                                "response for correlationId=" + this.correlationId + ", error: " +
                                controlResponseDecoder.ErrorMessage(),
                                (int) controlResponseDecoder.RelevantId(),
                                responseCorrelationId);

                            if (responseCorrelationId == this.correlationId)
                            {
                                throw ex;
                            }
                            else
                            {
                                errorHandler?.OnError(ex);
                            }
                        }
                    }

                    break;
                }

                case RecordingDescriptorDecoder.TEMPLATE_ID:
                {
                    recordingDescriptorDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderEncoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(),
                        messageHeaderDecoder.Version());

                    if (recordingDescriptorDecoder.ControlSessionId() == controlSessionId &&
                        recordingDescriptorDecoder.CorrelationId() == correlationId)
                    {
                        recordingDescriptorConsumer.OnRecordingDescriptor(
                            controlSessionId,
                            recordingDescriptorDecoder.CorrelationId(),
                            recordingDescriptorDecoder.RecordingId(),
                            recordingDescriptorDecoder.StartTimestamp(),
                            recordingDescriptorDecoder.StopTimestamp(),
                            recordingDescriptorDecoder.StartPosition(),
                            recordingDescriptorDecoder.StopPosition(),
                            recordingDescriptorDecoder.InitialTermId(),
                            recordingDescriptorDecoder.SegmentFileLength(),
                            recordingDescriptorDecoder.TermBufferLength(),
                            recordingDescriptorDecoder.MtuLength(),
                            recordingDescriptorDecoder.SessionId(),
                            recordingDescriptorDecoder.StreamId(),
                            recordingDescriptorDecoder.StrippedChannel(),
                            recordingDescriptorDecoder.OriginalChannel(),
                            recordingDescriptorDecoder.SourceIdentity());

                        if (0 == --remainingRecordCount)
                        {
                            isDispatchComplete = true;
                            return ControlledFragmentHandlerAction.BREAK;
                        }
                    }

                    break;
                }
                
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

            return ControlledFragmentHandlerAction.CONTINUE;
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return "RecordingDescriptorPoller{" +
                   "controlSessionId=" + controlSessionId +
                   ", correlationId=" + correlationId +
                   ", remainingRecordCount=" + remainingRecordCount +
                   ", isDispatchComplete=" + isDispatchComplete +
                   '}';
        }
    }
}