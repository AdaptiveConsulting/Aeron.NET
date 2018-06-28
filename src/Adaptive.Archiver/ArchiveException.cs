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
        
        /// <summary>
        /// Error code providing more detail into what went wrong.
        /// </summary>
        /// <returns> code providing more detail into what went wrong. </returns>
        public int ErrorCode { get; }
        
        public ArchiveException()
        {
            ErrorCode = GENERIC;
        }

        public ArchiveException(string message) : base(message)
        {
            ErrorCode = GENERIC;
        }

        public ArchiveException(string message, int errorCode) : base(message)
        {
            ErrorCode = errorCode;
        }

        public ArchiveException(string message, Exception innerException, int errorCode) : base(message, innerException)
        {
            ErrorCode = errorCode;
        }
    }
}