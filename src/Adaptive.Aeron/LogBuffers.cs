/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0S
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Util;

[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")]
namespace Adaptive.Aeron
{
    /// <summary>
    /// Takes a log file name and maps the file into memory and wraps it with <seealso cref="UnsafeBuffer"/>s as appropriate.
    /// </summary>
    /// <seealso cref="LogBufferDescriptor" />
    public class LogBuffers : IDisposable
    {
        private readonly int _termLength;
        private readonly UnsafeBuffer[] termBuffers = new UnsafeBuffer[LogBufferDescriptor.PARTITION_COUNT];
        private readonly UnsafeBuffer logMetaDataBuffer;
        private readonly MappedByteBuffer[] _mappedByteBuffers;

        internal LogBuffers()
        {
        }

        public LogBuffers(string logFileName, MapMode mapMode)
        {
            var fileInfo = new FileInfo(logFileName);

            var logLength = fileInfo.Length;
            var termLength = LogBufferDescriptor.ComputeTermLength(logLength);

            LogBufferDescriptor.CheckTermLength(termLength);

            _termLength = termLength;

            // if log length exceeds MAX_INT we need multiple mapped buffers, (see FileChannel.map doc).
            if (logLength < int.MaxValue)
            {
                var mappedBuffer = IoUtil.MapExistingFile(logFileName, mapMode);

                _mappedByteBuffers = new[] {mappedBuffer};
                
                for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
                {
                    termBuffers[i] = new UnsafeBuffer(mappedBuffer.Pointer, i * termLength, termLength);
                }

                logMetaDataBuffer = new UnsafeBuffer(mappedBuffer.Pointer, (int)(logLength - LogBufferDescriptor.LOG_META_DATA_LENGTH), LogBufferDescriptor.LOG_META_DATA_LENGTH);
            }
            else
            {
                _mappedByteBuffers = new MappedByteBuffer[LogBufferDescriptor.PARTITION_COUNT + 1];
                var memoryMappedFile = IoUtil.OpenMemoryMappedFile(logFileName, mapMode);
                
                for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
                {
                    _mappedByteBuffers[i] = new MappedByteBuffer(memoryMappedFile, termLength*(long) i, termLength);
                    termBuffers[i] = new UnsafeBuffer(_mappedByteBuffers[i].Pointer, 0, termLength);
                }

                var metaDataMappedBuffer = new MappedByteBuffer(memoryMappedFile, logLength - LogBufferDescriptor.LOG_META_DATA_LENGTH, LogBufferDescriptor.LOG_META_DATA_LENGTH);
                _mappedByteBuffers[_mappedByteBuffers.Length - 1] = metaDataMappedBuffer;
                logMetaDataBuffer = new UnsafeBuffer(metaDataMappedBuffer.Pointer, 0, LogBufferDescriptor.LOG_META_DATA_LENGTH);
            }

            // TODO try/catch
            
            foreach (var buffer in termBuffers)
            {
                buffer.VerifyAlignment();
            }

            logMetaDataBuffer.VerifyAlignment();
        }

#if DEBUG
        public virtual UnsafeBuffer[] TermBuffers()
#else
        public UnsafeBuffer[] TermBuffers()
#endif
        {
            return termBuffers;
        }

#if DEBUG
        public virtual UnsafeBuffer MetaDataBuffer()
#else
        public UnsafeBuffer MetaDataBuffer()
#endif
        {
            return logMetaDataBuffer;
        }

#if DEBUG
        public virtual void Dispose()
#else
        public void Dispose()
#endif
        {
            foreach (var buffer in _mappedByteBuffers)
            {
                IoUtil.Unmap(buffer);
            }
        }

#if DEBUG
        public virtual int TermLength()
#else
        public int TermLength()
#endif
        {
            return _termLength;
        }
    }
}