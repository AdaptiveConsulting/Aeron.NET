using System;
using Adaptive.Aeron.Exceptions;

namespace Adaptive.Archiver
{
    public class ArchiveException : AeronException
    {
        /// <summary>
        /// Generic archive error with detail likely in the message.
        /// </summary>
        public const int GENERIC = 0;

        /// <summary>
        /// An active listing of recordings is currently in operation on the session.
        /// </summary>
        public const int ACTIVE_LISTING = 1;

        /// <summary>
        /// The recording is currently active so the requested operation is not valid.
        /// </summary>
        public const int ACTIVE_RECORDING = 2;

        /// <summary>
        /// A subscription is currently active for the requested channel and stream id which would clash.
        /// </summary>
        public const int ACTIVE_SUBSCRIPTION = 3;

        /// <summary>
        /// The subscription for the requested operation is not known to the archive.
        /// </summary>
        public const int UNKNOWN_SUBSCRIPTION = 4;

        /// <summary>
        /// The recording identity for the operation is not know to the archive.
        /// </summary>
        public const int UNKNOWN_RECORDING = 5;

        /// <summary>
        /// The replay identity for the operation is not known to the archive.
        /// </summary>
        public const int UNKNOWN_REPLAY = 6;

        /// <summary>
        /// The archive has reached its maximum concurrent replay sessions.
        /// </summary>
        public const int MAX_REPLAYS = 7;

        /// <summary>
        /// The archive has reached its maximum concurrent recording sessions.
        /// </summary>
        public const int MAX_RECORDINGS = 8;

        /// <summary>
        /// The extend-recording operation is not valid for the existing recording.
        /// </summary>
        public const int INVALID_EXTENSION = 9;

        /// <summary>
        /// The archive is rejecting the session because of failed authentication.
        /// </summary>
        public const int AUTHENTICATION_REJECTED = 10;

        /// <summary>
        /// The archive storage is at minimum threshold or exhausted.
        /// </summary>
        public const int STORAGE_SPACE = 11;

        /// <summary>
        /// The replication identity for this operation is not known to the archive.
        /// </summary>
        public const int UNKNOWN_REPLICATION = 12;
        
        /// <summary>
        /// The principle was not authorised to take the requested action. 
        /// </summary>
        public const int UNAUTHORISED_ACTION = 13;

        /// <summary>
        /// Error code providing more detail into what went wrong.
        /// </summary>
        /// <returns> code providing more detail into what went wrong. </returns>
        public int ErrorCode { get; }
        
        /// <summary>
        /// Optional correlation-id associated with a control protocol request. Will be <seealso cref="Aeron.Aeron.NULL_VALUE"/> if
        /// not set.
        /// </summary>
        /// <returns> correlation-id associated with a control protocol request. </returns>
        public long CorrelationId { get; }

        /// <summary>
        /// Default ArchiveException exception as <seealso cref="Aeron.Exceptions.Category.ERROR"/> and
        /// <seealso cref="ErrorCode"/> = <seealso cref="GENERIC"/>.
        /// </summary>
        public ArchiveException()
        {
            ErrorCode = GENERIC;
            CorrelationId = Aeron.Aeron.NULL_VALUE;
        }

        /// <summary>
        /// ArchiveException exception as <seealso cref="Aeron.Exceptions.Category.ERROR"/> and
        /// <seealso cref="ErrorCode"/> = <seealso cref="GENERIC"/>, plus detail.
        /// </summary>
        /// <param name="message"> providing detail. </param>
        public ArchiveException(string message) : base(message)
        {
            ErrorCode = GENERIC;
            CorrelationId = Aeron.Aeron.NULL_VALUE;
        }

        /// <summary>
        /// ArchiveException exception as <seealso cref="Aeron.Exceptions.Category.ERROR"/>, plus detail and
        /// error code.
        /// </summary>
        /// <param name="message">   providing detail. </param>
        /// <param name="errorCode"> for type. </param>
        public ArchiveException(string message, int errorCode) : base(message)
        {
            ErrorCode = errorCode;
            CorrelationId = Aeron.Aeron.NULL_VALUE;
        }

        /// <summary>
        /// ArchiveException exception as <seealso cref="Aeron.Exceptions.Category.ERROR"/>, plus detail, error code,
        /// and correlation if of the control request.
        /// </summary>
        /// <param name="message">       providing detail. </param>
        /// <param name="errorCode">     for type. </param>
        /// <param name="correlationId"> of the control request. </param>
        public ArchiveException(string message, int errorCode, long correlationId) : base(message)
        {
            ErrorCode = errorCode;
            CorrelationId = correlationId;
        }
    }
}