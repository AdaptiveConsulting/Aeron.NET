using System;
using Adaptive.Aeron.Exceptions;

namespace Adaptive.Archiver
{
    public class ArchiveException : AeronException
    {
        public const int GENERIC = 0;
        public const int ACTIVE_LISTING = 1;
        public const int ACTIVE_RECORDING = 2;
        public const int ACTIVE_SUBSCRIPTION = 3;
        public const int UNKNOWN_SUBSCRIPTION = 4;
        public const int UNKNOWN_RECORDING = 5;
        public const int UNKNOWN_REPLAY = 6;
        public const int MAX_REPLAYS = 7;
        public const int MAX_RECORDINGS = 8;
        public const int INVALID_EXTENSION  = 9;
        public const int AUTHENTICATION_REJECTED = 10;
        
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
        
        public ArchiveException()
        {
            ErrorCode = GENERIC;
            CorrelationId = Aeron.Aeron.NULL_VALUE;
        }

        public ArchiveException(string message) : base(message)
        {
            ErrorCode = GENERIC;
            CorrelationId = Aeron.Aeron.NULL_VALUE;
        }

        public ArchiveException(string message, int errorCode) : base(message)
        {
            ErrorCode = errorCode;
            CorrelationId = Aeron.Aeron.NULL_VALUE;
        }

        public ArchiveException(string message, Exception innerException, int errorCode) : base(message, innerException)
        {
            ErrorCode = errorCode;
            CorrelationId = Aeron.Aeron.NULL_VALUE;
        }
        
        public ArchiveException(string message, int errorCode, long correlationId) : base(message)
        {
            ErrorCode = errorCode;
            CorrelationId = correlationId;
        }
    }
}