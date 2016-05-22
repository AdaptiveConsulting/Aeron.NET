using System;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;

namespace Adaptive.Agrona.Util
{
    /// <summary>
    /// Utility to copy blocks of memory
    /// </summary>
    public unsafe class ByteUtil
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void MemoryCopy(byte* destination, byte* source, uint length)
        {
            var src = (byte*)source;
            var dst = (byte*) destination;
            var len = length;
            var pos = 0;
            var len8 = len - 8;
            while (pos <= len8) {
                *(long*)(dst + pos) = *(long*)(src + pos);
                pos += 8;
            }
            var len4 = len - 4;
            while (pos <= len4) {
                *(int*)(dst + pos) = *(int*)(src + pos);
                pos += 4;
            }
            while (pos < len) {
                *(byte*)(dst + pos) = *(byte*)(src + pos);
                pos++;
            }
        }
        
    }
}