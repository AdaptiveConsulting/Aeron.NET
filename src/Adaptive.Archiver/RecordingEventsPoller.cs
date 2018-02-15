using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona.Concurrent;
using Adaptive.Archiver.Codecs;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Encapsulate the polling and decoding of recording events.
    /// </summary>
    public class RecordingEventsPoller : IFragmentHandler
    {
        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly RecordingStartedDecoder recordingStartedDecoder = new RecordingStartedDecoder();
        private readonly RecordingProgressDecoder recordingProgressDecoder = new RecordingProgressDecoder();
        private readonly RecordingStoppedDecoder recordingStoppedDecoder = new RecordingStoppedDecoder();

        private readonly Subscription subscription;
        private int templateId;
        private bool pollComplete;

        private long recordingId;
        private long recordingStartPosition;
        private long recordingPosition;
        private long recordingStopPosition;

        /// <summary>
        /// Create a poller for a given subscription to an archive for recording events.
        /// </summary>
        /// <param name="subscription"> to poll for new events. </param>
        public RecordingEventsPoller(Subscription subscription)
        {
            this.subscription = subscription;
        }

        /// <summary>
        /// Poll for recording events.
        /// </summary>
        /// <returns> the number of fragments read during the operation. Zero if no events are available. </returns>
        public virtual int Poll()
        {
            templateId = -1;
            pollComplete = false;

            return subscription.Poll(this, 1);
        }

        /// <summary>
        /// Has the last polling action received a complete message?
        /// </summary>
        /// <returns> true of the last polling action received a complete message? </returns>
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
        /// Get the recording id of the last received event.
        /// </summary>
        /// <returns> the recording id of the last received event. </returns>
        public virtual long RecordingId()
        {
            return recordingId;
        }

        /// <summary>
        /// Get the position the recording started at.
        /// </summary>
        /// <returns> the position the recording started at. </returns>
        public virtual long RecordingStartPosition()
        {
            return recordingStartPosition;
        }

        /// <summary>
        /// Get the current recording position.
        /// </summary>
        /// <returns> the current recording position. </returns>
        public virtual long RecordingPosition()
        {
            return recordingPosition;
        }

        /// <summary>
        /// Get the position the recording stopped at.
        /// </summary>
        /// <returns> the position the recording stopped at. </returns>
        public virtual long RecordingStopPosition()
        {
            return recordingStopPosition;
        }

        public virtual void OnFragment(UnsafeBuffer buffer, int offset, int length, Header header)
        {
            messageHeaderDecoder.Wrap(buffer, offset);

            templateId = messageHeaderDecoder.TemplateId();
            switch (templateId)
            {
                case RecordingStartedDecoder.TEMPLATE_ID:
                    recordingStartedDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    recordingId = recordingStartedDecoder.RecordingId();
                    recordingStartPosition = recordingStartedDecoder.StartPosition();
                    recordingPosition = recordingStartPosition;
                    recordingStopPosition = -1;
                    break;

                case RecordingProgressDecoder.TEMPLATE_ID:
                    recordingProgressDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    recordingId = recordingProgressDecoder.RecordingId();
                    recordingStartPosition = recordingProgressDecoder.StartPosition();
                    recordingPosition = recordingProgressDecoder.Position();
                    recordingStopPosition = -1;
                    break;

                case RecordingStoppedDecoder.TEMPLATE_ID:
                    recordingStoppedDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    recordingId = recordingStoppedDecoder.RecordingId();
                    recordingStartPosition = recordingStoppedDecoder.StartPosition();
                    recordingStopPosition = recordingStoppedDecoder.StopPosition();
                    recordingPosition = recordingStopPosition;
                    break;

                default:
                    throw new System.InvalidOperationException("Unknown templateId: " + templateId);
            }

            pollComplete = true;
        }
    }
}