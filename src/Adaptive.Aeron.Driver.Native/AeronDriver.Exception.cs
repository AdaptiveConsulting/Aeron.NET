using System;

namespace Adaptive.Aeron.Driver.Native
{
    public partial class AeronDriver
    {
        public class MediaDriverException : Exception
        {
            public MediaDriverException(string message)
                : base(message)
            {
            }
        }
    }
}