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
    public class LogBuffers : IDisposable, IManagedResource
    {
        private long _timeOfLastStateChangeNs;
        private int _refCount;
        
        private readonly int _termLength;
        private readonly UnsafeBuffer[] _termBuffers = new UnsafeBuffer[LogBufferDescriptor.PARTITION_COUNT];
        private readonly UnsafeBuffer _logMetaDataBuffer;
        private readonly MappedByteBuffer[] _mappedByteBuffers;

        internal LogBuffers()
        {
        }

        /// <summary>
        /// Construct the log buffers for a given log file.
        /// </summary>
        /// <param name="logFileName"></param>
        public LogBuffers(string logFileName)
        {
            try
            {
                var fileInfo = new FileInfo(logFileName);

                var logLength = fileInfo.Length;

                // if log length exceeds MAX_INT we need multiple mapped buffers, (see FileChannel.map doc).
                if (logLength < int.MaxValue)
                {
                    var mappedBuffer = IoUtil.MapExistingFile(logFileName, MapMode.ReadWrite); // TODO Java has sparse hint
                    _mappedByteBuffers = new[] {mappedBuffer};

                    _logMetaDataBuffer = new UnsafeBuffer(mappedBuffer.Pointer,
                        (int) (logLength - LogBufferDescriptor.LOG_META_DATA_LENGTH),
                        LogBufferDescriptor.LOG_META_DATA_LENGTH);

                    int termLength = LogBufferDescriptor.TermLength(_logMetaDataBuffer);
                    int pageSize = LogBufferDescriptor.PageSize(_logMetaDataBuffer);

                    LogBufferDescriptor.CheckTermLength(termLength);
                    LogBufferDescriptor.CheckPageSize(pageSize);

                    _termLength = termLength;

                    for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
                    {
                        _termBuffers[i] = new UnsafeBuffer(mappedBuffer.Pointer, i * termLength, termLength);
                    }
                }
                else
                {
                    _mappedByteBuffers = new MappedByteBuffer[LogBufferDescriptor.PARTITION_COUNT + 1];

                    int assumedTermLength = LogBufferDescriptor.TERM_MAX_LENGTH;
                    long metaDataSectionOffset = assumedTermLength * (long) LogBufferDescriptor.PARTITION_COUNT;
                    long metaDataMappingLength = logLength - metaDataSectionOffset;

                    var memoryMappedFile = IoUtil.OpenMemoryMappedFile(logFileName);

                    var metaDataMappedBuffer =
                        new MappedByteBuffer(memoryMappedFile, metaDataSectionOffset, metaDataMappingLength);
                    _mappedByteBuffers[_mappedByteBuffers.Length - 1] = metaDataMappedBuffer;
                    _logMetaDataBuffer = new UnsafeBuffer(
                        metaDataMappedBuffer.Pointer,
                        (int) metaDataMappingLength - LogBufferDescriptor.LOG_META_DATA_LENGTH,
                        LogBufferDescriptor.LOG_META_DATA_LENGTH);

                    int metaDataTermLength = LogBufferDescriptor.TermLength(_logMetaDataBuffer);
                    int pageSize = LogBufferDescriptor.PageSize(_logMetaDataBuffer);

                    LogBufferDescriptor.CheckPageSize(pageSize);
                    if (metaDataTermLength != assumedTermLength)
                    {
                        throw new InvalidOperationException(
                            $"Assumed term length {assumedTermLength} does not match metadta: termLength = {metaDataTermLength}");
                    }

                    _termLength = assumedTermLength;

                    for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
                    {
                        long position = assumedTermLength * (long) i;

                        _mappedByteBuffers[i] = new MappedByteBuffer(memoryMappedFile, position, assumedTermLength);
                        _termBuffers[i] = new UnsafeBuffer(_mappedByteBuffers[i].Pointer, 0, assumedTermLength);
                    }
                }
            }
            catch (InvalidOperationException ex)
            {
                Dispose();
                throw ex;
            }
        }

#if DEBUG
        public virtual UnsafeBuffer[] DuplicateTermBuffers()
#else
        public UnsafeBuffer[] DuplicateTermBuffers()
#endif
        {
            return _termBuffers;
        }

#if DEBUG
        public virtual UnsafeBuffer MetaDataBuffer()
#else
        /// <summary>
        /// Get the buffer which holds the log metadata.
        /// </summary>
        /// <returns> the buffer which holds the log metadata. </returns>
        public UnsafeBuffer MetaDataBuffer()
#endif
        {
            return _logMetaDataBuffer;
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
        /// <summary>
        ///  The length of the term buffer in each log partition.
        /// </summary>
        /// <returns> length of the term buffer in each log partition. </returns>
        public int TermLength()
#endif
        {
            return _termLength;
        }

        public int IncRef()
        {
            return ++_refCount;
        }

        public int DecRef()
        {
            return --_refCount;
        }

        public void TimeOfLastStateChange(long timeNs)
        {
            _timeOfLastStateChangeNs = timeNs;
        } 
        
        public long TimeOfLastStateChange()
        {
            return _timeOfLastStateChangeNs;
        }
        
        public void Delete()
        {
            Dispose();
        }
    }
}