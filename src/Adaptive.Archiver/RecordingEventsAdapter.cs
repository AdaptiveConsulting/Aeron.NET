using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Archiver.Codecs;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Encapsulate the polling, decoding, and dispatching of recording events.
    /// </summary>
    public class RecordingEventsAdapter : IFragmentHandler
    {
        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly RecordingStartedDecoder recordingStartedDecoder = new RecordingStartedDecoder();
        private readonly RecordingProgressDecoder recordingProgressDecoder = new RecordingProgressDecoder();
        private readonly RecordingStoppedDecoder recordingStoppedDecoder = new RecordingStoppedDecoder();

        private readonly int fragmentLimit;
        private readonly IRecordingEventsListener listener;
        private readonly Subscription subscription;

        /// <summary>
        /// Create a poller for a given subscription to an archive for recording events.
        /// </summary>
        /// <param name="listener">      to which events are dispatched. </param>
        /// <param name="subscription">  to poll for new events. </param>
        /// <param name="fragmentLimit"> to apply for each polling operation. </param>
        public RecordingEventsAdapter(IRecordingEventsListener listener, Subscription subscription, int fragmentLimit)
        {
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
            return subscription.Poll(this, fragmentLimit);
        }

        public virtual void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            messageHeaderDecoder.Wrap(buffer, offset);

            int templateId = messageHeaderDecoder.TemplateId();
            switch (templateId)
            {
                case RecordingStartedDecoder.TEMPLATE_ID:
                    recordingStartedDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    listener.OnStart(recordingStartedDecoder.RecordingId(), recordingStartedDecoder.StartPosition(), recordingStartedDecoder.SessionId(), recordingStartedDecoder.StreamId(), recordingStartedDecoder.Channel(), recordingStartedDecoder.SourceIdentity());
                    break;

                case RecordingProgressDecoder.TEMPLATE_ID:
                    recordingProgressDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    listener.OnProgress(recordingProgressDecoder.RecordingId(), recordingProgressDecoder.StartPosition(), recordingProgressDecoder.Position());
                    break;

                case RecordingStoppedDecoder.TEMPLATE_ID:
                    recordingStoppedDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    listener.OnStop(recordingStoppedDecoder.RecordingId(), recordingStoppedDecoder.StartPosition(), recordingStoppedDecoder.StopPosition());
                    break;

                default:
                    throw new System.InvalidOperationException("Unknown templateId: " + templateId);
            }
        }
    }
}