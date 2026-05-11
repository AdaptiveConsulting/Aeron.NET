using System.IO;

namespace Adaptive.Aeron.Exceptions
{
    using System;

    /// <summary>
    /// A request to allocate a resource (e.g. log buffer) failed due to insufficient storage space available.
    /// </summary>
    public class StorageSpaceException : AeronException
    {
        /// <summary>
        /// Construct the exception for the with detailed message.
        /// </summary>
        /// <param name="message"> detail for the exception. </param>
        public StorageSpaceException(string message) : base(message)
        {
        }

        /// <summary>
        /// Check if given exception denotes an out of disc space error, i.e. which on Linux is represented by error code
        /// {@code ENOSPC(28)} and on Windows by error code  {@code ERROR_DISK_FULL(112)}.
        /// </summary>
        /// <param name="error"> to check. </param>
        /// <returns> {@code true} if cause is <seealso cref="IOException"/> with a specific error. </returns>
        public static bool IsStorageSpaceError(Exception error)
        {
            Exception cause = error;
            while (null != cause)
            {
                if (cause is StorageSpaceException)
                {
                    return true;
                }

                if (cause is IOException)
                {
                    string message = cause.Message;
                    if (null != message &&
                        (message.Contains("No space left on device") ||
                         message.Contains("There is not enough space on the disk")))
                    {
                        return true;
                    }

                    if ((uint)cause.HResult == 0x80070070)
                    {
                        return true;
                    }
                }

                cause = cause.InnerException;
            }

            return false;
        }
    }
}