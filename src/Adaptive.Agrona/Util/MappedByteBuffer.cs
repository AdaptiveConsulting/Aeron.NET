using System;
using System.IO.MemoryMappedFiles;

namespace Adaptive.Agrona.Util
{
    public unsafe class MappedByteBuffer : IDisposable
    {
        private readonly MemoryMappedFile _memoryMappedFile;
        private readonly MemoryMappedViewAccessor _view;
        private bool _disposed;

        public MappedByteBuffer(MemoryMappedFile memoryMappedFile)
        {
            _memoryMappedFile = memoryMappedFile;
            byte* ptr = (byte*)0;
            _view = memoryMappedFile.CreateViewAccessor();
            _view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            
            Pointer = new IntPtr(ptr);
        }

        public MappedByteBuffer(MemoryMappedFile memoryMappedFile, long offset, int length)
        {
            _memoryMappedFile = memoryMappedFile;
            byte* ptr = (byte*)0;
            _view = memoryMappedFile.CreateViewAccessor(offset, length);
            _view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

            Pointer = new IntPtr(ptr);
        }

        public IntPtr Pointer { get; }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~MappedByteBuffer()
        {
            Dispose(false);
        }

        private void Dispose(bool disposing)
        {
            if (_disposed)
                return;
            if (_view != null)
            {
                _view.SafeMemoryMappedViewHandle.ReleasePointer();
                _view.Dispose();
            }
            _memoryMappedFile?.Dispose();

            _disposed = true;
        }
    }
}