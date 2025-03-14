using System;

namespace Adaptive.Aeron.Exceptions
{
    public class ControlProtocolException : AeronException
    {
        private readonly ErrorCode code;

        /// <summary>
        /// Construct an exception to indicate an invalid command has been sent to the media driver.
        /// </summary>
        /// <param name="code"> for the type of error. </param>
        /// <param name="msg">  providing more detail. </param>
        public ControlProtocolException(ErrorCode code, string msg) : base(msg)
        {
            this.code = code;
        }

        /// <summary>
        /// Construct an exception to indicate an invalid command has been sent to the media driver.
        /// </summary>
        /// <param name="code">      for the type of error. </param>
        /// <param name="rootCause"> of the error. </param>
        public ControlProtocolException(ErrorCode code, Exception rootCause) : base(rootCause)
        {
            this.code = code;
        }
        
        /// <summary>
        /// Construct an exception to indicate an invalid command has been sent to the media driver.
        /// </summary>
        /// <param name="code">      for the type of error. </param>
        /// <param name="msg">       providing more detail. </param>
        /// <param name="rootCause"> of the error. </param>
        public ControlProtocolException(ErrorCode code, string msg, Exception rootCause) : base(msg, rootCause)
        {
            this.code = code;
        }

        /// <summary>
        /// The <seealso cref="ErrorCode"/> indicating more specific issue experienced by the media driver.
        /// </summary>
        /// <returns> <seealso cref="Adaptive.Aeron.ErrorCode"/> indicating more specific issue experienced by the media driver. </returns>
        public ErrorCode ErrorCode()
        {
            return code;
        }
    }
}