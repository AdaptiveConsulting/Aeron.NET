using System;
using System.IO;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Util;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Takes a log file name and maps the file into memory and wraps it with <seealso cref="UnsafeBuffer"/>s as appropriate.
    /// </summary>
    /// <seealso cref="LogBufferDescriptor" />
    public class LogBuffers : IDisposable
    {
        private readonly int _termLength;
        private readonly UnsafeBuffer[] _atomicBuffers = new UnsafeBuffer[LogBufferDescriptor.PARTITION_COUNT*2 + 1];
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

                var metaDataSectionOffset = termLength*LogBufferDescriptor.PARTITION_COUNT;

                for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
                {
                    var metaDataOffset = metaDataSectionOffset + (i*LogBufferDescriptor.TERM_META_DATA_LENGTH);

                    _atomicBuffers[i] = new UnsafeBuffer(mappedBuffer.Pointer, i*termLength, termLength);
                    _atomicBuffers[i + LogBufferDescriptor.PARTITION_COUNT] = new UnsafeBuffer(mappedBuffer.Pointer, metaDataOffset, LogBufferDescriptor.TERM_META_DATA_LENGTH);
                }

                _atomicBuffers[_atomicBuffers.Length - 1] = new UnsafeBuffer(mappedBuffer.Pointer, (int) (logLength - LogBufferDescriptor.LOG_META_DATA_LENGTH), LogBufferDescriptor.LOG_META_DATA_LENGTH);
            }
            else
            {
                _mappedByteBuffers = new MappedByteBuffer[LogBufferDescriptor.PARTITION_COUNT + 1];
                var metaDataSectionOffset = termLength*(long) LogBufferDescriptor.PARTITION_COUNT;
                var metaDataSectionLength = (int) (logLength - metaDataSectionOffset);

                var memoryMappedFile = IoUtil.OpenMemoryMappedFile(logFileName, mapMode);
                var metaDataMappedBuffer = new MappedByteBuffer(memoryMappedFile, metaDataSectionOffset, metaDataSectionLength);

                _mappedByteBuffers[_mappedByteBuffers.Length - 1] = metaDataMappedBuffer;

                for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
                {
                    _mappedByteBuffers[i] = new MappedByteBuffer(memoryMappedFile, termLength*(long) i, termLength);

                    _atomicBuffers[i] = new UnsafeBuffer(_mappedByteBuffers[i].Pointer, termLength);
                    _atomicBuffers[i + LogBufferDescriptor.PARTITION_COUNT] = new UnsafeBuffer(metaDataMappedBuffer.Pointer, i*LogBufferDescriptor.TERM_META_DATA_LENGTH, LogBufferDescriptor.TERM_META_DATA_LENGTH);
                }

                _atomicBuffers[_atomicBuffers.Length - 1] = new UnsafeBuffer(metaDataMappedBuffer.Pointer, metaDataSectionLength - LogBufferDescriptor.LOG_META_DATA_LENGTH, LogBufferDescriptor.LOG_META_DATA_LENGTH);
            }

            // TODO try/catch
            
            foreach (var buffer in _atomicBuffers)
            {
                buffer.VerifyAlignment();
            }
        }

#if DEBUG
        public virtual UnsafeBuffer[] AtomicBuffers()
#else
        public UnsafeBuffer[] AtomicBuffers()
#endif
        {
            return _atomicBuffers;
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