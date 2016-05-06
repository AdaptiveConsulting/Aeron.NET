using System;
using System.IO.MemoryMappedFiles;

namespace Adaptive.Agrona.Util
{
    public unsafe class MemoryMappedFilePointer : IDisposable
    {
        private readonly MemoryMappedViewAccessor _view;

        public MemoryMappedFilePointer(MemoryMappedFile memoryMappedFile)
        {
            byte* ptr = (byte*)0;
            _view = memoryMappedFile.CreateViewAccessor();
            _view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            
            Pointer = new IntPtr(ptr);
        }

        public IntPtr Pointer { get; }

        public void Dispose()
        {
            _view.SafeMemoryMappedViewHandle.ReleasePointer();
            _view.Dispose();
        }
    }
}