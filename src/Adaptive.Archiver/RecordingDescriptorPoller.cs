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
        private bool InstanceFieldsInitialized = false;

        private void InitializeInstanceFields()
        {
            fragmentAssembler = new ControlledFragmentAssembler(this);
        }

        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();
        private readonly RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();

        private readonly int fragmentLimit;
        private readonly Subscription subscription;
        private ControlledFragmentAssembler fragmentAssembler;
        private readonly long controlSessionId;

        private long expectedCorrelationId;
        private int remainingRecordCount;
        private IRecordingDescriptorConsumer consumer;
        private bool isDispatchComplete = false;

        /// <summary>
        /// Create a poller for a given subscription to an archive for control response messages.
        /// </summary>
        /// <param name="subscription">     to poll for new events. </param>
        /// <param name="fragmentLimit">    to apply for each polling operation. </param>
        /// <param name="controlSessionId"> to filter the responses. </param>
        public RecordingDescriptorPoller(Subscription subscription, int fragmentLimit, long controlSessionId)
        {
            if (!InstanceFieldsInitialized)
            {
                InitializeInstanceFields();
                InstanceFieldsInitialized = true;
            }

            this.subscription = subscription;
            this.fragmentLimit = fragmentLimit;
            this.controlSessionId = controlSessionId;
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
            isDispatchComplete = false;

            return subscription.ControlledPoll(fragmentAssembler, fragmentLimit);
        }

        /// <summary>
        /// Control session id for filtering responses.
        /// </summary>
        /// <returns> control session id for filtering responses. </returns>
        public virtual long ControlSessionId()
        {
            return controlSessionId;
        }

        /// <summary>
        /// Is the dispatch of descriptors complete?
        /// </summary>
        /// <returns> true if the dispatch of descriptors complete? </returns>
        public virtual bool IsDispatchComplete()
        {
            return isDispatchComplete;
        }

        /// <summary>
        /// Get the number of remaining records are expected.
        /// </summary>
        /// <returns> the number of remaining records are expected. </returns>
        public virtual int RemainingRecordCount()
        {
            return remainingRecordCount;
        }

        /// <summary>
        /// Reset the poller to dispatch the descriptors returned from a query.
        /// </summary>
        /// <param name="expectedCorrelationId"> for the response. </param>
        /// <param name="recordCount">           of descriptors to expect. </param>
        /// <param name="consumer">              to which the recording descriptors are to be dispatched. </param>
        public virtual void Reset(long expectedCorrelationId, int recordCount, IRecordingDescriptorConsumer consumer)
        {
            this.expectedCorrelationId = expectedCorrelationId;
            this.consumer = consumer;
            this.remainingRecordCount = recordCount;
            isDispatchComplete = false;
        }

        public virtual ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            messageHeaderDecoder.Wrap(buffer, offset);

            int templateId = messageHeaderDecoder.TemplateId();
            switch (templateId)
            {
                case ControlResponseDecoder.TEMPLATE_ID:
                    controlResponseDecoder.Wrap(buffer, offset + MessageHeaderEncoder.ENCODED_LENGTH, messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    if (controlResponseDecoder.ControlSessionId() != controlSessionId)
                    {
                        break;
                    }

                    ControlResponseCode code = controlResponseDecoder.Code();

                    if (ControlResponseCode.RECORDING_UNKNOWN == code)
                    {
                        isDispatchComplete = true;
                        return ControlledFragmentHandlerAction.BREAK;
                    }

                    if (ControlResponseCode.ERROR == code)
                    {
                        throw new ArchiveException("response for expectedCorrelationId=" + expectedCorrelationId + ", error: " + controlResponseDecoder.ErrorMessage());
                    }

                    break;

                case RecordingDescriptorDecoder.TEMPLATE_ID:
                    recordingDescriptorDecoder.Wrap(buffer, offset + MessageHeaderEncoder.ENCODED_LENGTH, messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    long correlationId = recordingDescriptorDecoder.CorrelationId();
                    if (controlSessionId != recordingDescriptorDecoder.ControlSessionId() || correlationId != expectedCorrelationId)
                    {
                        break;
                    }

                    consumer.OnRecordingDescriptor(controlSessionId, correlationId, recordingDescriptorDecoder.RecordingId(), recordingDescriptorDecoder.StartTimestamp(), recordingDescriptorDecoder.StopTimestamp(), recordingDescriptorDecoder.StartPosition(), recordingDescriptorDecoder.StopPosition(), recordingDescriptorDecoder.InitialTermId(), recordingDescriptorDecoder.SegmentFileLength(), recordingDescriptorDecoder.TermBufferLength(), recordingDescriptorDecoder.MtuLength(), recordingDescriptorDecoder.SessionId(), recordingDescriptorDecoder.StreamId(), recordingDescriptorDecoder.StrippedChannel(), recordingDescriptorDecoder.OriginalChannel(), recordingDescriptorDecoder.SourceIdentity());

                    if (0 == --remainingRecordCount)
                    {
                        isDispatchComplete = true;
                        return ControlledFragmentHandlerAction.BREAK;
                    }

                    break;

                default:
                    throw new ArchiveException("unknown templateId: " + templateId);
            }

            return ControlledFragmentHandlerAction.CONTINUE;
        }
    }
}