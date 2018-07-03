using System.Runtime.CompilerServices;
using Adaptive.Aeron.Exceptions;

namespace Adaptive.Aeron
{
    public class AeronThrowHelper
    {
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ThrowAeronException(string message)
        {
            throw GetAeronException(message);
        }
        
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static AeronException GetAeronException(string message)
        {
            return new AeronException(message);
        }
    }
}