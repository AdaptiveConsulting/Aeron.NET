﻿using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Message to denote that new buffers have been setup for a publication.
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
    /// |                    Publication Limit Offset                   |
    /// +---------------------------------------------------------------+
    /// |                         Log File Length                       |
    /// +---------------------------------------------------------------+
    /// |                          Log File Name                      ...
    /// ...                                                             |
    /// +---------------------------------------------------------------+ 
    public class PublicationBuffersReadyFlyweight
    {
        private const int CORRELATION_ID_OFFSET = 0;
        private static readonly int SESSION_ID_OFFSET = CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
        private static readonly int STREAM_ID_FIELD_OFFSET = SESSION_ID_OFFSET + BitUtil.SIZE_OF_INT;
        private static readonly int PUBLICATION_LIMIT_COUNTER_ID_OFFSET = STREAM_ID_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
        private static readonly int LOGFILE_FIELD_OFFSET = PUBLICATION_LIMIT_COUNTER_ID_OFFSET + BitUtil.SIZE_OF_INT;

        private UnsafeBuffer _buffer;
        private int _offset;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> for fluent API </returns>
        public PublicationBuffersReadyFlyweight Wrap(UnsafeBuffer buffer, int offset)
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
        public PublicationBuffersReadyFlyweight CorrelationId(long correlationId)
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
        /// set session id field
        /// </summary>
        /// <param name="sessionId"> field value </param>
        /// <returns> flyweight </returns>
        public PublicationBuffersReadyFlyweight SessionId(int sessionId)
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
        public PublicationBuffersReadyFlyweight StreamId(int streamId)
        {
            _buffer.PutInt(_offset + STREAM_ID_FIELD_OFFSET, streamId);

            return this;
        }

        /// <summary>
        /// The publication limit counter id.
        /// </summary>
        /// <returns> publication limit counter id. </returns>
        public int PublicationLimitCounterId()
        {
            return _buffer.GetInt(_offset + PUBLICATION_LIMIT_COUNTER_ID_OFFSET);
        }

        /// <summary>
        /// set position counter id field
        /// </summary>
        /// <param name="positionCounterId"> field value </param>
        /// <returns> flyweight </returns>
        public PublicationBuffersReadyFlyweight PublicationLimitCounterId(int positionCounterId)
        {
            _buffer.PutInt(_offset + PUBLICATION_LIMIT_COUNTER_ID_OFFSET, positionCounterId);

            return this;
        }

        public string LogFileName()
        {
            return _buffer.GetStringUtf8(_offset + LOGFILE_FIELD_OFFSET);
        }

        public PublicationBuffersReadyFlyweight LogFileName(string logFileName)
        {
            _buffer.PutStringUtf8(_offset + LOGFILE_FIELD_OFFSET, logFileName);
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
            return _buffer.GetInt(_offset + LOGFILE_FIELD_OFFSET) + LOGFILE_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
        }

    }
}