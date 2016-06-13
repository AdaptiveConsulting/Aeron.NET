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
    }
}