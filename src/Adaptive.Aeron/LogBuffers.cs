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
        private long lingerDeadlineNs = long.MaxValue;
        private int _refCount;

        private readonly int _termLength;
        private readonly UnsafeBuffer[] _termBuffers = new UnsafeBuffer[LogBufferDescriptor.PARTITION_COUNT];
        private readonly UnsafeBuffer _logMetaDataBuffer;
        private readonly MappedByteBuffer[] _mappedByteBuffers;

        // ReSharper disable once UnusedMember.Global
        //
        // Necessary for testing
        internal LogBuffers()
        {
            
        }
        
        /// <summary>
        /// Construct the log buffers for a given log file.
        /// </summary>
        /// <param name="logFileName"></param>
        public LogBuffers(string logFileName)
        {
            int termLength = 0;
            UnsafeBuffer logMetaDataBuffer = null;
            MappedByteBuffer[] mappedByteBuffers = null;

            try
            {
                var fileInfo = new FileInfo(logFileName);

                var logLength = fileInfo.Length;

                // if log length exceeds MAX_INT we need multiple mapped buffers, (see FileChannel.map doc).
                if (logLength < int.MaxValue)
                {
                    var mappedBuffer =
                        IoUtil.MapExistingFile(logFileName,
                            MapMode.ReadWrite); // TODO Java has sparse hint & Little Endian
                    mappedByteBuffers = new[] {mappedBuffer};

                    logMetaDataBuffer = new UnsafeBuffer(mappedBuffer.Pointer,
                        (int) (logLength - LogBufferDescriptor.LOG_META_DATA_LENGTH),
                        LogBufferDescriptor.LOG_META_DATA_LENGTH);

                    termLength = LogBufferDescriptor.TermLength(logMetaDataBuffer);
                    int pageSize = LogBufferDescriptor.PageSize(logMetaDataBuffer);

                    LogBufferDescriptor.CheckTermLength(termLength);
                    LogBufferDescriptor.CheckPageSize(pageSize);

                    for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
                    {
                        _termBuffers[i] = new UnsafeBuffer(mappedBuffer.Pointer, i * termLength, termLength);
                    }
                }
                else
                {
                    mappedByteBuffers = new MappedByteBuffer[LogBufferDescriptor.PARTITION_COUNT + 1];

                    int assumedTermLength = LogBufferDescriptor.TERM_MAX_LENGTH;
                    long metaDataSectionOffset = assumedTermLength * (long) LogBufferDescriptor.PARTITION_COUNT;
                    long metaDataMappingLength = logLength - metaDataSectionOffset;

                    var memoryMappedFile = IoUtil.OpenMemoryMappedFile(logFileName);

                    var metaDataMappedBuffer =
                        new MappedByteBuffer(memoryMappedFile, metaDataSectionOffset,
                            metaDataMappingLength); // Little Endian

                    mappedByteBuffers[LogBufferDescriptor.LOG_META_DATA_SECTION_INDEX] = metaDataMappedBuffer;
                    logMetaDataBuffer = new UnsafeBuffer(
                        metaDataMappedBuffer.Pointer,
                        (int) metaDataMappingLength - LogBufferDescriptor.LOG_META_DATA_LENGTH,
                        LogBufferDescriptor.LOG_META_DATA_LENGTH);

                    int metaDataTermLength = LogBufferDescriptor.TermLength(logMetaDataBuffer);
                    int pageSize = LogBufferDescriptor.PageSize(logMetaDataBuffer);

                    LogBufferDescriptor.CheckPageSize(pageSize);
                    if (metaDataTermLength != assumedTermLength)
                    {
                        throw new InvalidOperationException(
                            $"assumed term length {assumedTermLength} does not match metadta: termLength = {metaDataTermLength}");
                    }

                    termLength = assumedTermLength;

                    for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
                    {
                        long position = assumedTermLength * (long) i;

                        mappedByteBuffers[i] =
                            new MappedByteBuffer(memoryMappedFile, position, assumedTermLength); // Little Endian
                        _termBuffers[i] = new UnsafeBuffer(mappedByteBuffers[i].Pointer, 0, assumedTermLength);
                    }
                }
            }
            catch (InvalidOperationException)
            {
                Dispose();
                throw;
            }
            
            _termLength = termLength;
            _logMetaDataBuffer = logMetaDataBuffer;
            _mappedByteBuffers = mappedByteBuffers;
        }

        public UnsafeBuffer[] DuplicateTermBuffers()
        {
            return _termBuffers;
        }

        /// <summary>
        /// Get the buffer which holds the log metadata.
        /// </summary>
        /// <returns> the buffer which holds the log metadata. </returns>
        public UnsafeBuffer MetaDataBuffer()
        {
            return _logMetaDataBuffer;
        }

        /// <summary>
        /// Pre touch memory pages, so they are faulted in to be available before access.
        /// </summary>
        public void PreTouch()
        {
            const int value = 0;
            int pageSize = LogBufferDescriptor.PageSize(_logMetaDataBuffer);
            UnsafeBuffer atomicBuffer = new UnsafeBuffer();

            foreach (MappedByteBuffer buffer in _mappedByteBuffers)
            {
                atomicBuffer.Wrap(buffer.Pointer, 0, (int) buffer.Capacity);

                for (int i = 0, length = atomicBuffer.Capacity; i < length; i += pageSize)
                {
                    atomicBuffer.CompareAndSetInt(i, value, value);
                }
            }
        }

        public void Dispose()
        {
            var length = _mappedByteBuffers.Length;
            for (var i = 0; i < length; i++)
            {
                var buffer = _mappedByteBuffers[i];
                IoUtil.Unmap(buffer);
                _mappedByteBuffers[i] = null;
            }
            
            _logMetaDataBuffer.Wrap(0, 0);
        }

        /// <summary>
        ///  The length of the term buffer in each log partition.
        /// </summary>
        /// <returns> length of the term buffer in each log partition. </returns>
        public int TermLength()
        {
            return _termLength;
        }

        /// <summary>
        /// Increment reference count.
        /// </summary>
        /// <returns> current reference count after increment. </returns>
        public int IncRef()
        {
            return ++_refCount;
        }

        /// <summary>
        /// Decrement reference count.
        /// </summary>
        /// <returns> current reference counter after decrement. </returns>
        public int DecRef()
        {
            return --_refCount;
        }

        /// <summary>
        /// Set the deadline for how long to linger around once unreferenced.
        /// </summary>
        /// <param name="timeNs"> the deadline for how long to linger around once unreferenced. </param>
        public void LingerDeadlineNs(long timeNs)
        {
            lingerDeadlineNs = timeNs;
        }

        /// <summary>
        /// The deadline for how long to linger around once unreferenced.
        /// </summary>
        /// <returns> the deadline for how long to linger around once unreferenced. </returns>
        public long LingerDeadlineNs()
        {
            return lingerDeadlineNs;
        }
    }
}