using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Archiver.Codecs;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Encapsulate the polling, decoding, and dispatching of recording events.
    /// </summary>
    public class RecordingEventsAdapter : IFragmentHandler
    {
        private readonly MessageHeaderDecoder _messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly RecordingStartedDecoder _recordingStartedDecoder = new RecordingStartedDecoder();
        private readonly RecordingProgressDecoder _recordingProgressDecoder = new RecordingProgressDecoder();
        private readonly RecordingStoppedDecoder _recordingStoppedDecoder = new RecordingStoppedDecoder();

        private readonly int _fragmentLimit;
        private readonly IRecordingEventsListener _listener;
        private readonly Subscription _subscription;

        /// <summary>
        /// Create an adapter for a given subscription to an archive for recording events.
        /// </summary>
        /// <param name="listener">      to which events are dispatched. </param>
        /// <param name="subscription">  to poll for new events. </param>
        /// <param name="fragmentLimit"> to apply for each polling operation. </param>
        public RecordingEventsAdapter(IRecordingEventsListener listener, Subscription subscription, int fragmentLimit)
        {
            _fragmentLimit = fragmentLimit;
            _listener = listener;
            _subscription = subscription;
        }

        /// <summary>
        /// Poll for recording events and dispatch them to the <seealso cref="IRecordingEventsListener"/> for this instance.
        /// </summary>
        /// <returns> the number of fragments read during the operation. Zero if no events are available. </returns>
        public int Poll()
        {
            return _subscription.Poll(this, _fragmentLimit);
        }

        public void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            _messageHeaderDecoder.Wrap(buffer, offset);

            int schemaId = _messageHeaderDecoder.SchemaId();
            if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
            {
                throw new ArchiveException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" +
                                           schemaId);
            }

            int templateId = _messageHeaderDecoder.TemplateId();
            switch (templateId)
            {
                case RecordingStartedDecoder.TEMPLATE_ID:
                    _recordingStartedDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version());

                    _listener.OnStart(
                        _recordingStartedDecoder.RecordingId(),
                        _recordingStartedDecoder.StartPosition(),
                        _recordingStartedDecoder.SessionId(),
                        _recordingStartedDecoder.StreamId(),
                        _recordingStartedDecoder.Channel(),
                        _recordingStartedDecoder.SourceIdentity());
                    break;

                case RecordingProgressDecoder.TEMPLATE_ID:
                    _recordingProgressDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version());

                    _listener.OnProgress(
                        _recordingProgressDecoder.RecordingId(),
                        _recordingProgressDecoder.StartPosition(),
                        _recordingProgressDecoder.Position());
                    break;

                case RecordingStoppedDecoder.TEMPLATE_ID:
                    _recordingStoppedDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version());

                    _listener.OnStop(
                        _recordingStoppedDecoder.RecordingId(),
                        _recordingStoppedDecoder.StartPosition(),
                        _recordingStoppedDecoder.StopPosition());
                    break;
            }
        }
    }
}