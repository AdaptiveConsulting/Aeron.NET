/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
namespace Adaptive.Agrona.Util {

    /// <summary>
    /// Utility to copy blocks of memory
    /// </summary>
    public unsafe class ByteUtil {

        // frames are 32-bytes aligned, without CopyChunk64 throughput is better

        //[StructLayout(LayoutKind.Sequential, Pack = 64, Size = 64)]
        //internal struct CopyChunk64 {
        //    private fixed byte _bytes[64];
        //}

        [StructLayout(LayoutKind.Sequential, Pack = 32, Size = 32)]
        internal struct CopyChunk32
        {
            private readonly long _l1;
            private readonly long _l2;
            private readonly long _l3;
            private readonly long _l4;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void MemoryCopy(byte* destination, byte* source, uint length) {
            var pos = 0;
            int nextPos;
            // whithout this perf is better
            //nextPos = pos + 64;
            //while (nextPos <= length) {
            //    *(CopyChunk64*)(destination + pos) = *(CopyChunk64*)(source + pos);
            //    pos = nextPos;
            //    nextPos += 64;
            //}
            nextPos = pos + 32;
            while (nextPos <= length) {
                *(CopyChunk32*)(destination + pos) = *(CopyChunk32*)(source + pos);
                pos = nextPos;
                nextPos += 32;
            }
            nextPos = pos + 8;
            while (nextPos <= length) {
                *(long*)(destination + pos) = *(long*)(source + pos);
                pos = nextPos;
                nextPos += 8;
            }
            // whithout this perf is better
            //nextPos = pos + 4;
            //while (nextPos <= length) {
            //    *(int*)(destination + pos) = *(int*)(source + pos);
            //    pos = nextPos;
            //    nextPos += 4;
            //}
            while (pos < length) {
                *(byte*)(destination + pos) = *(byte*)(source + pos);
                pos++;
            }
        }

    }
}