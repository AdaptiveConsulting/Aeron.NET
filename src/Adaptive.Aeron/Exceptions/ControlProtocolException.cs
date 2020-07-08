using System;

namespace Adaptive.Aeron.Exceptions
{
    public class ControlProtocolException : AeronException
    {
        private readonly ErrorCode code;

        public ControlProtocolException(ErrorCode code, string msg) : base(msg)
        {
            this.code = code;
        }

        public ControlProtocolException(ErrorCode code, Exception rootCause) : base(rootCause)
        {
            this.code = code;
        }

        public ErrorCode ErrorCode()
        {
            return code;
        }
    }
}