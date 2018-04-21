using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Archiver.Codecs;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Encapsulate the polling, decoding, and dispatching of archive control protocol response messages.
    /// </summary>
    public class ControlResponseAdapter : IFragmentHandler
    {
        private bool InstanceFieldsInitialized = false;

        private void InitializeInstanceFields()
        {
            fragmentAssembler = new FragmentAssembler(this);
        }

        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();
        private readonly RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();

        private readonly int fragmentLimit;
        private readonly IControlResponseListener listener;
        private readonly Subscription subscription;
        private FragmentAssembler fragmentAssembler;

        /// <summary>
        /// Create an adapter for a given subscription to an archive for control response messages.
        /// </summary>
        /// <param name="listener">      to which responses are dispatched. </param>
        /// <param name="subscription">  to poll for new events. </param>
        /// <param name="fragmentLimit"> to apply for each polling operation. </param>
        public ControlResponseAdapter(IControlResponseListener listener, Subscription subscription, int fragmentLimit)
        {
            if (!InstanceFieldsInitialized)
            {
                InitializeInstanceFields();
                InstanceFieldsInitialized = true;
            }

            this.fragmentLimit = fragmentLimit;
            this.listener = listener;
            this.subscription = subscription;
        }

        /// <summary>
        /// Poll for recording events and dispatch them to the <seealso cref="RecordingEventsListener"/> for this instance.
        /// </summary>
        /// <returns> the number of fragments read during the operation. Zero if no events are available. </returns>
        public virtual int Poll()
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
            consumer.OnRecordingDescriptor(decoder.ControlSessionId(), decoder.CorrelationId(), decoder.RecordingId(), decoder.StartTimestamp(), decoder.StopTimestamp(), decoder.StartPosition(), decoder.StopPosition(), decoder.InitialTermId(), decoder.SegmentFileLength(), decoder.TermBufferLength(), decoder.MtuLength(), decoder.SessionId(), decoder.StreamId(), decoder.StrippedChannel(), decoder.OriginalChannel(), decoder.SourceIdentity());
        }

        public virtual void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            messageHeaderDecoder.Wrap(buffer, offset);

            int templateId = messageHeaderDecoder.TemplateId();
            switch (templateId)
            {
                case ControlResponseDecoder.TEMPLATE_ID:
                    HandleControlResponse(listener, buffer, offset);
                    break;

                case RecordingDescriptorDecoder.TEMPLATE_ID:
                    HandleRecordingDescriptor(listener, buffer, offset);
                    break;

                default:
                    throw new InvalidOperationException("unknown templateId: " + templateId);
            }
        }

        private void HandleControlResponse(IControlResponseListener listener, IDirectBuffer buffer, int offset)
        {
            controlResponseDecoder.Wrap(buffer, offset + MessageHeaderEncoder.ENCODED_LENGTH, messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

            listener.OnResponse(controlResponseDecoder.ControlSessionId(), controlResponseDecoder.CorrelationId(), controlResponseDecoder.RelevantId(), controlResponseDecoder.Code(), controlResponseDecoder.ErrorMessage());
        }

        private void HandleRecordingDescriptor(IControlResponseListener listener, IDirectBuffer buffer, int offset)
        {
            recordingDescriptorDecoder.Wrap(buffer, offset + MessageHeaderEncoder.ENCODED_LENGTH, messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

            DispatchDescriptor(recordingDescriptorDecoder, listener);
        }
    }
}