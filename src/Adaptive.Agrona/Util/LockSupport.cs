using System.Runtime.CompilerServices;
using System.Threading;

namespace Adaptive.Agrona.Util
{
    /// <summary>
    /// Mimic Java class LockSupport
    /// </summary>
    public static class LockSupport
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ParkNanos(int nanos)
        {
            // TODO check closest equivalent in .NET
            Thread.Sleep(1);
        }
    }
}
