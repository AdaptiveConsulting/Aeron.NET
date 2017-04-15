using Adaptive.Agrona;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Message to denote that new buffers have been added for a subscription.
    /// 
    /// NOTE: Layout should be SBE compliant
    /// </summary>
    /// <seealso cref="ControlProtocolEvents" />
    /// 
    /// 0                   1                   2                   3
    /// 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    /// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /// |                         Correlation ID                        |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// |                          Session ID                           |
    /// +---------------------------------------------------------------+
    /// |                           Stream ID                           |
    /// +---------------------------------------------------------------+
    /// |                Subscriber Position Block Length               |
    /// +---------------------------------------------------------------+
    /// |                   Subscriber Position Count                   |
    /// +---------------------------------------------------------------+
    /// |                      Subscriber Position Id 0                 |
    /// +---------------------------------------------------------------+
    /// |                         Registration Id 0                     |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// |                     Subscriber Position Id 1                  |
    /// +---------------------------------------------------------------+
    /// |                         Registration Id 1                     |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// |                                                              ...
    /// ...     Up to "Position Indicators Count" entries of this form  |
    /// +---------------------------------------------------------------+
    /// |                         Log File Length                       |
    /// +---------------------------------------------------------------+
    /// |                          Log File Name (ASCII)               ...
    /// ...                                                             |
    /// +---------------------------------------------------------------+
    /// |                     Source identity Length                    |
    /// +---------------------------------------------------------------+
    /// |                     Source identity (ASCII)                  ...
    /// ...                                                             |
    /// +---------------------------------------------------------------+ 
    public class ImageBuffersReadyFlyweight
    {
        private static readonly int CORRELATION_ID_OFFSET;
        private static readonly int SESSION_ID_OFFSET;
        private static readonly int STREAM_ID_FIELD_OFFSET;
        private static readonly int SUBSCRIBER_POSITION_BLOCK_LENGTH_OFFSET;
        private static readonly int SUBSCRIBER_POSITION_COUNT_OFFSET;
        private static readonly int SUBSCRIBER_POSITIONS_OFFSET;

        private static readonly int SUBSCRIBER_POSITION_BLOCK_LENGTH;

        static ImageBuffersReadyFlyweight()
        {
            CORRELATION_ID_OFFSET = 0;
            SESSION_ID_OFFSET = CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
            STREAM_ID_FIELD_OFFSET = SESSION_ID_OFFSET + BitUtil.SIZE_OF_INT;
            SUBSCRIBER_POSITION_BLOCK_LENGTH_OFFSET = STREAM_ID_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
            SUBSCRIBER_POSITION_COUNT_OFFSET = SUBSCRIBER_POSITION_BLOCK_LENGTH_OFFSET + BitUtil.SIZE_OF_INT;
            SUBSCRIBER_POSITIONS_OFFSET = SUBSCRIBER_POSITION_COUNT_OFFSET + BitUtil.SIZE_OF_INT;
            SUBSCRIBER_POSITION_BLOCK_LENGTH = BitUtil.SIZE_OF_LONG + BitUtil.SIZE_OF_INT;
        }

        private IMutableDirectBuffer _buffer;
        private int _offset;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> for fluent API </returns>
        public ImageBuffersReadyFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            this._buffer = buffer;
            this._offset = offset;

            return this;
        }

        /// <summary>
        /// return correlation id field
        /// </summary>
        /// <returns> correlation id field </returns>
        public long CorrelationId()
        {
            return _buffer.GetLong(_offset + CORRELATION_ID_OFFSET);
        }

        /// <summary>
        /// set correlation id field
        /// </summary>
        /// <param name="correlationId"> field value </param>
        /// <returns> flyweight </returns>
        public ImageBuffersReadyFlyweight CorrelationId(long correlationId)
        {
            _buffer.PutLong(_offset + CORRELATION_ID_OFFSET, correlationId);

            return this;
        }

        /// <summary>
        /// return session id field
        /// </summary>
        /// <returns> session id field </returns>
        public int SessionId()
        {
            return _buffer.GetInt(_offset + SESSION_ID_OFFSET);
        }

        /// <summary>
        /// set session id field </summary>
        /// <param name="sessionId"> field value </param>
        /// <returns> flyweight </returns>
        public ImageBuffersReadyFlyweight SessionId(int sessionId)
        {
            _buffer.PutInt(_offset + SESSION_ID_OFFSET, sessionId);

            return this;
        }

        /// <summary>
        /// return stream id field
        /// </summary>
        /// <returns> stream id field </returns>
        public int StreamId()
        {
            return _buffer.GetInt(_offset + STREAM_ID_FIELD_OFFSET);
        }

        /// <summary>
        /// set stream id field
        /// </summary>
        /// <param name="streamId"> field value </param>
        /// <returns> flyweight </returns>
        public ImageBuffersReadyFlyweight StreamId(int streamId)
        {
            _buffer.PutInt(_offset + STREAM_ID_FIELD_OFFSET, streamId);

            return this;
        }

        /// <summary>
        /// return the number of position indicators
        /// </summary>
        /// <returns> the number of position indicators </returns>
        public int SubscriberPositionCount()
        {
            return _buffer.GetInt(_offset + SUBSCRIBER_POSITION_COUNT_OFFSET);
        }

        /// <summary>
        /// set the number of position indicators
        /// </summary>
        /// <param name="value"> the number of position indicators </param>
        /// <returns> flyweight </returns>
        public ImageBuffersReadyFlyweight SubscriberPositionCount(int value)
        {
            _buffer.PutInt(_offset + SUBSCRIBER_POSITION_BLOCK_LENGTH_OFFSET, SUBSCRIBER_POSITION_BLOCK_LENGTH);
            _buffer.PutInt(_offset + SUBSCRIBER_POSITION_COUNT_OFFSET, value);

            return this;
        }

        /// <summary>
        /// Set the position Id for the subscriber
        /// </summary>
        /// <param name="index"> for the subscriber position </param>
        /// <param name="id"> for the subscriber position </param>
        /// <returns> flyweight </returns>
        public ImageBuffersReadyFlyweight SubscriberPositionId(int index, int id)
        {
            _buffer.PutInt(_offset + SubscriberPositionOffset(index), id);

            return this;
        }

        /// <summary>
        /// Return the position Id for the subscriber
        /// </summary>
        /// <param name="index"> for the subscriber position </param>
        /// <returns> position Id for the subscriber </returns>
        public int SubscriberPositionId(int index)
        {
            return _buffer.GetInt(_offset + SubscriberPositionOffset(index));
        }

        /// <summary>
        /// Set the registration Id for the subscriber position
        /// </summary>
        /// <param name="index"> for the subscriber position </param>
        /// <param name="id"> for the subscriber position </param>
        /// <returns> flyweight </returns>
        public ImageBuffersReadyFlyweight PositionIndicatorRegistrationId(int index, long id)
        {
            _buffer.PutLong(_offset + SubscriberPositionOffset(index) + BitUtil.SIZE_OF_INT, id);

            return this;
        }

        /// <summary>
        /// Return the registration Id for the subscriber position
        /// </summary>
        /// <param name="index"> for the subscriber position </param>
        /// <returns> registration Id for the subscriber position </returns>
        public long PositionIndicatorRegistrationId(int index)
        {
            return _buffer.GetLong(_offset + SubscriberPositionOffset(index) + BitUtil.SIZE_OF_INT);
        }

        /// <summary>
        /// Return the Log Filename in ASCII
        /// </summary>
        /// <returns> log filename </returns>
        public string LogFileName()
        {
            return _buffer.GetStringAscii(_offset + LogFileNameOffset());
        }

        /// <summary>
        /// Set the log filename in ASCII
        /// </summary>
        /// <param name="logFileName"> for the image </param>
        /// <returns> flyweight </returns>
        public ImageBuffersReadyFlyweight LogFileName(string logFileName)
        {
            _buffer.PutStringAscii(_offset + LogFileNameOffset(), logFileName);
            return this;
        }

        /// <summary>
        /// Return the source identity string in ASCII
        /// </summary>
        /// <returns> source identity string </returns>
        public string SourceIdentity()
        {
            return _buffer.GetStringAscii(_offset + SourceIdentityOffset());
        }

        /// <summary>
        /// Set the source identity string in ASCII
        /// </summary>
        /// <param name="value"> for the source identity </param>
        /// <returns> flyweight </returns>
        public ImageBuffersReadyFlyweight SourceIdentity(string value)
        {
            _buffer.PutStringAscii(_offset + SourceIdentityOffset(), value);
            return this;
        }

        /// <summary>
        /// Get the length of the current message
        /// 
        /// NB: must be called after the data is written in order to be accurate.
        /// </summary>
        /// <returns> the length of the current message </returns>
        public int Length()
        {
            int sourceIdentityOffset = SourceIdentityOffset();
            return sourceIdentityOffset + _buffer.GetInt(_offset + sourceIdentityOffset) + BitUtil.SIZE_OF_INT;
        }

        private int SubscriberPositionOffset(int index)
        {
            return SUBSCRIBER_POSITIONS_OFFSET + (index*SUBSCRIBER_POSITION_BLOCK_LENGTH);
        }

        private int LogFileNameOffset()
        {
            return SubscriberPositionOffset(SubscriberPositionCount());
        }

        private int SourceIdentityOffset()
        {
            int logFileNameOffset = LogFileNameOffset();
            return logFileNameOffset + _buffer.GetInt(_offset + logFileNameOffset) + BitUtil.SIZE_OF_INT;
        }
    }
}