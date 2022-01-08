using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Archiver.Codecs;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Encapsulate the polling, decoding, and dispatching of archive control protocol response messages.
    /// </summary>
    public class ControlResponseAdapter
    {
        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();
        private readonly RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();
        private readonly RecordingSignalEventDecoder recordingSignalEventDecoder = new RecordingSignalEventDecoder();

        private readonly int fragmentLimit;
        private readonly IControlResponseListener controlResponseListener;
        private readonly IRecordingSignalConsumer recordingSignalConsumer;
        private readonly Subscription subscription;
        private readonly FragmentAssembler fragmentAssembler;

        /// <summary>
        /// Create an adapter for a given subscription to an archive for control response messages.
        /// </summary>
        /// <param name="controlResponseListener">      to which responses are dispatched. </param>
        /// <param name="subscription">  to poll for new events. </param>
        /// <param name="fragmentLimit"> to apply for each polling operation. </param>
        public ControlResponseAdapter(
            IControlResponseListener controlResponseListener, 
            Subscription subscription, 
            int fragmentLimit) 
        : this(
                controlResponseListener, 
                AeronArchive.Configuration.NO_OP_RECORDING_SIGNAL_CONSUMER, 
                subscription, 
                fragmentLimit)
        {
        }
        
        /// <summary>
        /// Create an adapter for a given subscription to an archive for control response messages.
        /// </summary>
        /// <param name="controlResponseListener"> for dispatching responses. </param>
        /// <param name="recordingSignalConsumer"> for dispatching recording signals. </param>
        /// <param name="subscription">            to poll for responses. </param>
        /// <param name="fragmentLimit">           to apply for each polling operation. </param>
        public ControlResponseAdapter(
            IControlResponseListener controlResponseListener, 
            IRecordingSignalConsumer recordingSignalConsumer, 
            Subscription subscription, 
            int fragmentLimit)
        {
            fragmentAssembler = new FragmentAssembler(OnFragment);
            
            this.fragmentLimit = fragmentLimit;
            this.controlResponseListener = controlResponseListener;
            this.recordingSignalConsumer = recordingSignalConsumer;
            this.subscription = subscription;
        }


        /// <summary>
        /// Poll for recording events and dispatch them to the <seealso cref="IControlResponseListener"/> for this instance.
        /// </summary>
        /// <returns> the number of fragments read during the operation. Zero if no events are available. </returns>
        public int Poll()
        {
            return subscription.Poll(fragmentAssembler, fragmentLimit);
        }

        /// <summary>
        /// Dispatch a descriptor message to a consumer by reading the fields in the correct order.
        /// </summary>
        /// <param name="decoder">  which wraps the encoded message ready for reading. </param>
        /// <param name="consumer"> to which the decoded fields should be passed. </param>
        public static void DispatchDescriptor(RecordingDescriptorDecoder decoder, IRecordingDescriptorConsumer consumer)
        {
            consumer.OnRecordingDescriptor(
                decoder.ControlSessionId(),
                decoder.CorrelationId(),
                decoder.RecordingId(),
                decoder.StartTimestamp(),
                decoder.StopTimestamp(),
                decoder.StartPosition(),
                decoder.StopPosition(),
                decoder.InitialTermId(),
                decoder.SegmentFileLength(),
                decoder.TermBufferLength(),
                decoder.MtuLength(),
                decoder.SessionId(),
                decoder.StreamId(),
                decoder.StrippedChannel(),
                decoder.OriginalChannel(),
                decoder.SourceIdentity());
        }

        private void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            messageHeaderDecoder.Wrap(buffer, offset);

            int schemaId = messageHeaderDecoder.SchemaId();
            if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
            {
                throw new ArchiveException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
            }

            switch (messageHeaderDecoder.TemplateId())
            {
                case ControlResponseDecoder.TEMPLATE_ID:
                    HandleControlResponse(controlResponseListener, buffer, offset);
                    break;

                case RecordingDescriptorDecoder.TEMPLATE_ID:
                    HandleRecordingDescriptor(controlResponseListener, buffer, offset);
                    break;
                
                case RecordingSignalEventDecoder.TEMPLATE_ID:
                    HandleRecordingSignal(recordingSignalConsumer, buffer, offset);
                    break;
            }
        }

        private void HandleControlResponse(
            IControlResponseListener listener, IDirectBuffer buffer, int offset)
        {
            controlResponseDecoder.Wrap(
                buffer,
                offset + MessageHeaderEncoder.ENCODED_LENGTH,
                messageHeaderDecoder.BlockLength(),
                messageHeaderDecoder.Version());

            listener.OnResponse(
                controlResponseDecoder.ControlSessionId(),
                controlResponseDecoder.CorrelationId(),
                controlResponseDecoder.RelevantId(),
                controlResponseDecoder.Code(),
                controlResponseDecoder.ErrorMessage());
        }

        private void HandleRecordingDescriptor(
            IControlResponseListener listener, IDirectBuffer buffer, int offset)
        {
            recordingDescriptorDecoder.Wrap(
                buffer,
                offset + MessageHeaderEncoder.ENCODED_LENGTH,
                messageHeaderDecoder.BlockLength(),
                messageHeaderDecoder.Version());

            DispatchDescriptor(recordingDescriptorDecoder, listener);
        }
        
        private void HandleRecordingSignal(
            IRecordingSignalConsumer recordingSignalConsumer, IDirectBuffer buffer, int offset)
        {
            recordingSignalEventDecoder.Wrap(
                buffer, 
                offset + MessageHeaderDecoder.ENCODED_LENGTH, 
                messageHeaderDecoder.BlockLength(), 
                messageHeaderDecoder.Version());

            recordingSignalConsumer.OnSignal(
                recordingSignalEventDecoder.ControlSessionId(), 
                recordingSignalEventDecoder.CorrelationId(), 
                recordingSignalEventDecoder.RecordingId(), 
                recordingSignalEventDecoder.SubscriptionId(), 
                recordingSignalEventDecoder.Position(), 
                recordingSignalEventDecoder.Signal());
        }

    }
}