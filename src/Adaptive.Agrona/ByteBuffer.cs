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

using System;
using System.Runtime.InteropServices;

namespace Adaptive.Agrona
{
    public class ByteBuffer : IDisposable
    {
        private GCHandle _bufferHandle;
        private bool _disposed;
        public IntPtr BufferPointer { get; }
        public int Capacity { get; }

        public ByteBuffer(int capacity, int byteAlignment)
        {
            Capacity = capacity;
            var buffer = new byte[capacity + byteAlignment];
            _bufferHandle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
            var ptr = _bufferHandle.AddrOfPinnedObject().ToInt64();
            // round up ptr to nearest 'byteAlignment' boundary
            ptr = (ptr + byteAlignment - 1) & ~(byteAlignment - 1);
            BufferPointer = new IntPtr(ptr);
        }
        
        ~ByteBuffer()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
        }

        private void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            _bufferHandle.Free();

            _disposed = true;
        }

        public byte Get(int index)
        {
            unsafe
            {
                return *((byte*)BufferPointer.ToPointer() + index);
            }
        }
    }
}