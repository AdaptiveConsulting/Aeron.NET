using Adaptive.Aeron.Exceptions;

namespace Adaptive.Aeron.Driver.Native
{
    public partial class AeronDriver
    {
        public class MediaDriverException : AeronException
        {
            public MediaDriverException(string message)
                : base(message)
            {
            }
        }
    }
}