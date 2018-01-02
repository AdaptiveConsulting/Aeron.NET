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

        public MappedByteBuffer(MemoryMappedFile memoryMappedFile, long offset, long length)
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